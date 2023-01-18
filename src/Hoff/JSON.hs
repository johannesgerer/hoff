{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE QuasiQuotes #-}

-- * unsable API!
--
-- work in progress. not all useful formats and encode/decode steps are implemented for all types yet
--
-- enforces identical column types (obviously)
--
-- How JSON values are handled:
-- 
--   Array columns -> Maybe (Vector Value)
--
--   Object columns -> Maybe (KeyMap Value)
--
--   Number -> Maybe Scientific

module Hoff.JSON where

import           Control.DeepSeq
import           Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import           Data.Aeson.Key as A (toString, toText, fromText)
import           Data.Aeson.KeyMap as A (toAscList, toList, KeyMap)
import           Data.Aeson.Parser (eitherDecodeWith)
import           Data.Aeson.Types as A
import qualified Data.List as L
import qualified Data.Record.Anon as R
import qualified Data.Record.Anon.Advanced as R
import           Data.Scientific
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as M
import           Hoff.Dict
import           Hoff.H
import           Hoff.HQuery.Execution (exec)
import           Hoff.HQuery.Operators (rowDict)
import qualified Hoff.Serialise as HS
import           Hoff.Table
import           Hoff.TypedTable
import qualified Prelude
import           System.IO.Unsafe
import           Text.InterpolatedString.Perl6
import           Yahp as Y

type EoJTable = HS.EncodeOnlyJ (ColumnToVectorMap Table)

unsafeTableFromColumnToVectorMap :: LByteString -> Table
unsafeTableFromColumnToVectorMap = either (Prelude.error . toS) fromColumnToVectorMap . eitherDecode

unsafeTableFromVectorOfRecords :: LByteString -> Table
unsafeTableFromVectorOfRecords = either (Prelude.error . toS) fromVectorOfRecords . eitherDecode

unsafeTableKeyToRecordMap :: LByteString -> SingleKeyTable Text
unsafeTableKeyToRecordMap = either (Prelude.error . toS) fromKeyToRecordMap . eitherDecode


-- Javascript helpers:
--
-- // flip a list of (column name -> value) mapping to a mapping: column name -> column values
-- function flipObject(a) { return Object.values(a)[0].map((x,i) => Object.fromEntries(Object.entries(a).map(k => [k[0],k[1][i]]))) }
-- function where(columnVectors, colname, predicate)
-- {
--   const es = Object.entries(columnVectors);
--   return columnVectors[colname].reduce((a, e, i) => {if(predicate(e)) a.push(i); return a}, [])
--                          .map(i => Object.fromEntries(es.map(k => [k[0],k[1][i]])))
-- }


-- | 
--   {
--     "Account Number" : ["ABC15797531", null],
--     "b" :[null, 2.2],
--     "c" : [1,2]
--   }
--
-- 
newtype ColumnToVectorMap t = ColumnToVectorMap       { fromColumnToVectorMap   :: t }

-- |
--  [
--  {
--        "c1" : "",
--        "c2" :null
--  },
--  {
--        "c2" : null,
--        "c1" : 2.2
--   }
--  ,
--  {
--        "c1" : null,
--        "c2" : ""
--  }
--  ]
newtype VectorOfRecords t = VectorOfRecords       { fromVectorOfRecords   :: t }

-- | 
--  {
--     "k1" : {
--        "c1" : "",
--        "c2" :null
--     },
--     "k2" : {
--        "c2" : null,
--        "c1" : 2.2
--     }
--  ,
--     "k3" : {
--        "c1" : null,
--        "c2" : ""
--     }
--  }
newtype KeyToRecordMap t = KeyToRecordMap         { fromKeyToRecordMap    :: t }

instance FromJSON (KeyToRecordMap (SingleKeyTable Text)) where
  parseJSON = fmap KeyToRecordMap . parseKeyToRecordMap []

instance FromJSON (VectorOfRecords Table) where
  parseJSON = fmap VectorOfRecords . parseVectorOfRecords []

parseKeyToRecordMap :: [Text] -> Value -> Parser (SingleKeyTable Text)
parseKeyToRecordMap nulls = withObject "Hoff.Table via KeyToRecordMap"
  $ parseInIO . keyToRecordMap nulls

parseVectorOfRecords :: [Text] -> Value -> Parser Table
parseVectorOfRecords nulls = withArray "Hoff.Table via VectorOfRecords" $
    \v -> parseInIO $ parseRecords nulls v V.head V.iforM_

-- | catches TableJSONErrors
parseInIO :: MonadFail f => IO a -> f a
parseInIO p = unsafePerformIO $ catch (pure <$> p) $ pure . fail . fromTableJSONError 

parseRecords :: Foldable a => [Text] -> a Value -> (a Value -> Value) ->
  (forall m . Monad m => a Value -> (Int -> Value -> m ())-> m ()) -> IO Table
parseRecords nulls v head iterate = runHWith (throw . TableJSONError . show) pure . tableNoLengthCheck . dictNoLengthCheck colNames
  =<< V.zipWithM (vectorColumn $ V.fromList nulls) colNames colVectors
  where (colNames, colVectors) = fromRecords v head iterate

seqElements :: Vector a -> b -> b
seqElements = seq . liftRnf (\x -> seq x ())

keyToRecordMap :: [Text] -> Object -> IO (SingleKeyTable Text)
keyToRecordMap nulls o = ffor tableM $ \table -> dictNoLengthCheck (V.fromList $ A.toText <$> keys) table
  where tableM = parseRecords nulls records L.head $ \v f -> zipWithM_ f [0..] v
        (keys, records) = unzip $ A.toAscList o

-- -- | throws TableJSONError
fromRecords :: Foldable a => a Value -> (a Value -> Value) ->
  (forall m . Monad m => a Value -> (Int -> Value -> m ())-> m ()) -> (Symbols, Vector (Vector Value))
fromRecords vals head iterate
  | Y.null vals = throw $ TableJSONError "Empty JSON list, expected: List of records"
  | True = (V.fromList $ toText <$> colNames,) $ runST $ do
      cols <- V.replicateM ncols (M.unsafeNew nrows)
      let g rowId m = k (A.toAscList m) colNames 0
            where k [] []                      _ = pass
                  k ((rc,x):xs) (cname:cnames) c
                    | rc == cname = M.unsafeWrite (V.unsafeIndex cols c) rowId x >> k xs cnames (succ c)
                    | True        = throw $ TableJSONError
                                    $ "Row " <> show (succ rowId) <> " has an unexpected column " <> toString rc <> " expected " <> toString cname
                  k _ _                        _ = throw $ TableJSONError
                                                  $ "Row " <> show (succ rowId) <> " has " <> show (length m) <> " columns. Expected: " <> show ncols
      iterate vals $ \rowId -> g rowId . expectObject rowId
      mapM V.unsafeFreeze cols
  where ncols = length firstRow
        firstRow = expectObject 0 $ head vals
        colNames = fmap fst $ A.toAscList firstRow :: [Key]
        nrows = length vals

-- -- | throws TableJSONError
vectorColumn :: Vector Text -> Symbol -> Vector Value -> IO TableCol
vectorColumn nulls colname = vectorToTableCol (throwIO . TableJSONError) handle colname
  where handle :: (forall a . Int -> String -> String -> IO a) -> Value -> Maybe (Vector Value -> IO TableCol)
        handle err v = case v of
          String s -> guard (not $ V.elem s nulls) $> wrap (\i -> \case
            String t -> pure2 t;
            Null -> pure Nothing
            y -> err i (aesonTypeOf v) $ aesonTypeOf y)
          Number _ -> Just $ wrap $ \i -> \case
            Number t -> pure2 (t :: Scientific)
            Null -> pure Nothing
            y -> nullsOrError i y
          Bool   _ -> Just $ wrap $ \i -> \case
            Bool t   -> pure2 t
            Null -> pure Nothing
            y -> nullsOrError i y
          Null     -> Nothing
          Array  _ -> Just $ wrap $ \i -> \case
            Array t  -> pure2 (t :: Vector Value)
            Null -> pure Nothing 
            y -> nullsOrError i y
          Object  _ -> Just $ wrap $ \i -> \case
            Object t  -> pure2 (t :: KeyMap Value)
            Null -> pure Nothing 
            y -> nullsOrError i y
          where nullsOrError :: Int -> Value -> IO (Maybe b)
                nullsOrError rowId = \case
                  String y      | V.elem y nulls -> pure Nothing
                  y             -> err rowId (aesonTypeOf v) $ aesonTypeOf y

expectObject :: Int -> Value -> Object
expectObject rowId = \case
  Object o -> o
  v        -> throw $ TableJSONError $ "Row " <> show (succ rowId) <> " is not an object: " ++ show v

data TableJSONError = TableJSONError { fromTableJSONError :: String }
  deriving (Generic, Show)

instance Exception TableJSONError
  


input2 :: LByteString
input2 = [q|
[
   {
      "Name" : "Xytrex Co.",
      "Description" : "Industrial Cleaning Supply Company",
      "Account Number" : "ABC15797531",
      "b" :null
   },
   {
      "Name" : "Watson and Powell, Inc.",
      "Account Number" : null,
      "b" : 2.3,
      "Description" : "Industrial Cleaning Supply Company"
   }
]
|]

input1 :: LByteString
input1 = [q|
[
   {
      "Account Number" : "ABC15797531",
      "b" :null,
      "c" : 1
   },
   {
      "Account Number" : null,
      "b" : 2.2,
      "c": 2
   }
]
|]

type Input1R = '[ "Account Number" := Maybe String, "c" := Integer, "b" := Maybe Float ]

input1flip :: LByteString
input1flip = [q|
{
   "Account Number" : ["ABC15797531", null],
   "b" :[null, 2.2],
   "c" : [1,2]
}
|]

input3 :: LByteString
input3 = [q|
{
   "k1" : {
      "c1" : "",
      "c2" :null
   },
   "k2" : {
      "c2" : null,
      "c1" : "2.2"
   }
,
   "iun" : {
      "c1" : null,
      "c2" : ""
   }
}
|]

noneExample :: LByteString
noneExample = [q|
{
   "Account Number" : [],
   "b" :[],
   "c" : []
}
|]


 
parseInput1 :: IO ()
parseInput1 = either (\x -> print True >> putStrLn x)
  ((\x -> print (meta @Table x) >> print x) . fromVectorOfRecords) $ eitherDecode input1 
 

parseNoneExample :: IO ()
parseNoneExample = either (\x -> print True >> putStrLn x)
  ((\x -> print (meta @Table x) >> print x) . fromColumnToVectorMap @Table) $ eitherDecode noneExample 

parseInput1Flipped :: IO ()
parseInput1Flipped = either (\x -> print True >> putStrLn x)
  ((\x -> print (meta @Table x) >> print x) . fromColumnToVectorMap @Table) $ eitherDecode input1flip 

parseInput1Typed :: IO ()
parseInput1Typed = putStrLn . encode <=< either (throw . userError . show)
  (\x -> x <$ print (fromColumnToVectorMap @(TypedTable Input1R) x)) $ eitherDecode input1flip 
 
parseKeyToRecordMap1 :: IO ()
parseKeyToRecordMap1 = either (\x -> print True >> putStrLn x)
  ((\(x :: SingleKeyTable Text) -> print (meta $ value x) >> print x)
   . fromKeyToRecordMap) $ eitherDecode input3

nulls1 :: IO ()
nulls1 = either (\x -> print True >> print x)
  ((\x -> print (meta $ value x) >> print x)) $
  (eitherDecodeWith json' $ iparse $ parseKeyToRecordMap [""]) input3

eitherDecodeWith' :: (Value -> Parser a) -> LByteString -> Either Text a
eitherDecodeWith' p = left (toS . snd) . (eitherDecodeWith json' $ iparse p)

decodeVectorOfRecordsWith :: [Text] -> LByteString -> Either Text Table
decodeVectorOfRecordsWith = eitherDecodeWith' . parseVectorOfRecords

decodeKeyToRecordMapWith :: [Text] -> LByteString -> Either Text (SingleKeyTable Text)
decodeKeyToRecordMapWith = eitherDecodeWith' . parseKeyToRecordMap

-- main = parseKeyToRecordMap1
-- main = nulls1
-- main = parseInput1

-- | JSON type of a value, name of the head constructor.
aesonTypeOf :: Value -> String
aesonTypeOf v = case v of
    Object _ -> "Object"
    Array _  -> "Array"
    String _ -> "String"
    Number _ -> "Number"
    Bool _   -> "Boolean"
    Null     -> "Null"

instance FromJSON (ColumnToVectorMap Table) where
  parseJSON = withObject "Hoff.Table via ColumnToVectorMap" $
    fmap ColumnToVectorMap . runHorFail . table <=< mapM (g . first toText) . A.toList
    where g (k,v) = (k,) <$> withArray ("Column " <> toS k) (parseInIO . vectorColumn mempty k) v

-- | use via `Hoff.Serialise.ensureToJson`
instance HS.ToJsonAble (ColumnToVectorMap Table) where
  toJsonWithError (ColumnToVectorMap t) = appendCallStack
    . left (\e -> e <> "\n\n" <> showMetaWithDimensions t) . HS.toJsonWithError $ flipTable t

-- | not an efficiently implementation
instance HS.ToJsonAble (VectorOfRecords Table) where
  toJsonWithError (VectorOfRecords t) = appendCallStack
    . bimap (\e -> e <> "\n\n" <> showMetaWithDimensions t) (A.toEncoding &&& A.toJSON)
    . mapM HS.ensureToJson <=< runHEither $ exec rowDict t

-- | use via `Hoff.Serialise.ensureToJson`
instance HS.EncodableContainer f => HS.ToJsonAble (TableDict' f) where
  toJsonWithError t = fmap (f . V.unzip)
    $ V.zipWithM (\k v -> ((k,) *** (fromText k,)) . snd
                   <$> HS.encodeCol (HS.NamedWrappedDyn k v)) (key t) $ value t
    where f (es,vs) = (A.dict A.text id (V.foldr . uncurry) es, object $ Y.toList vs)


instance (TableRowT r, AllFields r ToJSON) => ToJSON (ColumnToVectorMap (TypedTable r)) where
  toEncoding = A.dict A.string id (foldr . uncurry)
    . R.toList . R.cmap (Proxy @ToJSON) (R.K . toEncoding) . columnRecord . fromColumnToVectorMap
  toJSON  = object . fmap (first fromString)
    . R.toList . R.cmap (Proxy @ToJSON) (R.K . toJSON) . columnRecord . fromColumnToVectorMap

instance (TableRowT r, AllFields r FromJSON) => FromJSON (ColumnToVectorMap (TypedTable r)) where
  parseJSON = withObject "Hoff.TypedTable via ColumnToVectorMap" $ \o -> do
    runHWith (fail . show) (pure . ColumnToVectorMap) . typedTable
      <=< R.cmapM (Proxy @FromJSON) (\(K name) -> o .: fromString name) $ R.reifyKnownFields Proxy
            

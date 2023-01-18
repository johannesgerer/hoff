{-# LANGUAGE QuasiQuotes #-}

module Hoff.JSON where

import           Control.DeepSeq
import           Control.Lens
import           Data.Aeson as A
import           Data.Aeson.Internal as A
import           Data.Aeson.Key as A (toString, toText)
import           Data.Aeson.KeyMap as A (toAscList)
import           Data.Aeson.Parser (eitherDecodeWith)
import           Data.Aeson.Types as A
import qualified Data.List as L
import           Data.Scientific
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as M
import           Hoff.Dict
import           Hoff.H
import           Hoff.Table
import           System.IO.Unsafe
import           Text.InterpolatedString.Perl6
import           Yahp as Y

newtype VectorOfRecords = VectorOfRecords       { fromVectorOfRecords   :: Maybe Table }
newtype KeyToRecordMap = KeyToRecordMap         { fromKeyToRecordMap    :: Maybe (SingleKeyTable Text) }

instance FromJSON KeyToRecordMap where parseJSON = fmap KeyToRecordMap . parseKeyToRecordMap []

instance FromJSON VectorOfRecords where parseJSON = fmap VectorOfRecords . parseVectorOfRecords []

parseKeyToRecordMap :: [Text] -> Value -> Parser (Maybe (SingleKeyTable Text))
parseKeyToRecordMap nulls = withObject "Hoff.Table via KeyToRecordMap" g
    where g v = unsafePerformIO $ catch (pure2 <$> res) $ pure . fail . fromTableJSONError 
            where res = keyToRecordMap nulls v

parseVectorOfRecords :: [Text] -> Value -> Parser (Maybe Table)
parseVectorOfRecords nulls = withArray "Hoff.Table via VectorOfRecords" g
    where g v = unsafePerformIO $ catch (pure2 <$> res) $ pure . fail . fromTableJSONError 
            where res = parseRecords nulls v V.head V.iforM_

parseRecords :: Foldable a => [Text] -> a Value -> (a Value -> Value) ->
  (forall m . Monad m => a Value -> (Int -> Value -> m ())-> m ()) -> IO Table
parseRecords nulls v head iterate = runHinIO . tableNoLengthCheck . dictNoLengthCheck colNames
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
fromRecords vals head iterate = (V.fromList $ toText <$> colNames,) $ runST $ do
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
  where context = "Column " <> toS colname
        handle :: (forall a . Int -> String -> String -> IO a) -> Value -> Maybe (Vector Value -> IO TableCol)
        handle err v = case v of
          String s -> guard (V.elem s nulls) $> wrap (\i -> \case
            String t -> pure2 t;
            Null -> pure Nothing
            y -> err i (aesonTypeOf v) $ aesonTypeOf y)
          Number _ -> Just $ wrap $ \i -> \case
            Number t -> pure2 $ toRealFloat @Double t
            Null -> pure Nothing
            y -> nullsOrError i y
          Bool   _ -> Just $ wrap $ \i -> \case
            Bool t   -> pure2 t
            Null -> pure Nothing
            y -> nullsOrError i y
          Null     -> Nothing
          _        -> Just $ const $ throwIO $ TableJSONError
            $ context <> ": 'FromJSON Hoff.Table' does not support " <> aesonTypeOf v
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
      "b" :null
   },
   {
      "Account Number" : null,
      "b" : 2.2
   }
]
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
      "c1" : 2.2
   }
,
   "iun" : {
      "c1" : null,
      "c2" : ""
   }
}
|]


 
parseInput1 :: IO ()
parseInput1 = either (\x -> print True >> putStrLn x)
  (maybe (print ("empty table"::Text)) (\x -> print (meta x) >> print x) . fromVectorOfRecords) $ eitherDecode input1 
 
parseKeyToRecordMap1 :: IO ()
parseKeyToRecordMap1 = either (\x -> print True >> putStrLn x)
  (maybe (print ("empty table"::Text)) (\x -> print (meta $ value x) >> print x) . fromKeyToRecordMap) $ eitherDecode input3

nulls1 :: IO ()
nulls1 = either (\x -> print True >> print x)
  (maybe (print ("empty table"::Text)) (\x -> print (meta $ value x) >> print x)) $
  (eitherDecodeWith json' $ iparse $ parseKeyToRecordMap [""]) input3

decodeVectorOfRecordsWith :: [Text] -> LByteString -> Either String (Maybe Table)
decodeVectorOfRecordsWith nulls = (_Left %~ snd) . (eitherDecodeWith json' $ iparse $ parseVectorOfRecords nulls)

decodeKeyToRecordMapWith :: [Text] -> LByteString -> Either String (Maybe (SingleKeyTable Text))
decodeKeyToRecordMapWith nulls = (_Left %~ snd) . (eitherDecodeWith json' $ iparse $ parseKeyToRecordMap nulls)

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

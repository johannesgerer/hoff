{-# LANGUAGE ViewPatterns #-}
-- needed for ?callStack
{-# LANGUAGE ImplicitParams #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fplugin=Data.Record.Anon.Plugin #-}


module Hoff.Serialise
  (module Hoff.Serialise
  ,module Codec.Serialise
  )
where

import qualified Chronos as C
import           Codec.CBOR.Decoding
import           Codec.CBOR.FlatTerm as C
import           Codec.CBOR.Pretty as C
import           Codec.CBOR.Read as C
import           Codec.CBOR.Term as C
import           Codec.Serialise
import           Codec.Serialise.Class
import           Codec.Serialise.Encoding as C
import           Control.Lens
import           Control.Monad.Writer
import           Data.Aeson (ToJSON)
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Base64.Lazy as BL64
import           Data.Coerce
import           Data.Maybe
import qualified Data.Record.Anon as R
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy.Encoding as TL
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import           Hoff.Dict
import           Hoff.H
import           Hoff.Table
import           Hoff.TypedTable
import           Paths_hoff (getDataFileName)
import qualified Prelude as Unsafe
import qualified Text.Hex as H
import           Type.Reflection
import           Yahp as Y hiding (TypeRep, typeRep)

-- * Serialisable helper class for values that do not have a (non-partial) encode function

data EncodeOnly a b = UnsafeEncodeOnly a b
type EncodeOnlyS a = EncodeOnly a C.Encoding

type EoTable = EncodeOnlyS Table

fromEncodeOnly :: EncodeOnly a b -> a
fromEncodeOnly (UnsafeEncodeOnly a _) = a

data DecodeOnly a = DecodeOnly { fromDecodeOnly :: a }

-- * Codec.Serialise 
ensureEncodable :: (HasCallStack, Encodable a) => a -> Either Text (EncodeOnlyS a)
ensureEncodable a = UnsafeEncodeOnly a <$> encodeWithError a

type Serialisable a = (Encodable a, Decodable a)

class Encodable a where
  encodeWithError       :: HasCallStack => a -> Either Text C.Encoding
  default encodeWithError :: (HasCallStack, Serialise a) => a -> Either Text C.Encoding
  encodeWithError = Right . encode

class Decodable a where
  decode'               :: Decoder s a
  default decode'       :: Serialise a => Decoder s a
  decode' = decode
  
instance Encodable a => Serialise (EncodeOnlyS a) where
  encode (UnsafeEncodeOnly _ e) = e
  decode = fail "Trying to decode a EncodeOnlyS value" 

instance Decodable a => Serialise (DecodeOnly a) where
  encode = Unsafe.error "Trying to decode a DecodeOnly value" 
  decode = DecodeOnly <$> decode' 

-- * Aeson
--
-- Use Hoff.JSON's newtypes to get ToJsonAble instances for tables

type EncodeOnlyJ a = EncodeOnly a (A.Encoding, A.Value)

ensureToJson :: (HasCallStack, ToJsonAble a) => a -> Either Text (EncodeOnlyJ a)
ensureToJson a = UnsafeEncodeOnly a <$> toJsonWithError a

class ToJsonAble a where
  toJsonWithError :: HasCallStack => a -> Either Text (A.Encoding, A.Value)
  default toJsonWithError :: (HasCallStack, A.ToJSON a) => a -> Either Text (A.Encoding, A.Value)
  toJsonWithError = Right . (A.toEncoding &&& A.toJSON)

instance ToJsonAble a => A.ToJSON (EncodeOnlyJ a) where
  toJSON (UnsafeEncodeOnly _ (_,v)) = v
  toEncoding (UnsafeEncodeOnly _ (e,_)) = e

-- tablesFromHex :: Text -> [(Text, Table)]
-- tablesFromHex = deserialise . fromHex

toDiagnostic :: Text -> String
toDiagnostic = toDiagnostic' . deserialise @C.Term . fromHex

printDiagnostic :: Text -> IO ()
printDiagnostic = putStrLn . toDiagnostic


toDiagnostic' :: Serialise a => a -> String
toDiagnostic' = C.prettyHexEnc . encode 

printDiagnostic' :: Serialise a => a -> IO ()
printDiagnostic' = putStrLn . toDiagnostic'

deriving instance Serialise C.Time
instance Serialise None where
  encode _ = encodeNull
  decode = coerce decodeNull

instance A.ToJSON None where
  toEncoding _ = A.null_ 
  toJSON _ = A.Null

instance Hashable C.Datetime where
  hashWithSalt s = hashWithSalt s . C.datetimeToTime


encodeVector' :: (VG.Vector v a) => (a -> C.Encoding) -> v a -> C.Encoding
encodeVector' encode = encodeContainerSkel encodeListLen VG.length VG.foldr (\a b -> encode a <> b)
{-# INLINE encodeVector' #-}

encodeVectorM' :: (a -> Either Text C.Encoding) -> Vector a -> Either Text C.Encoding
encodeVectorM' encodeM = fmap (\es -> encodeListLen (fromIntegral $ VG.length es) <> VG.foldMap id es)
  . noErrors . mapTellErrors encodeM
{-# INLINABLE encodeVectorM' #-}

decodeVector' :: (VG.Vector v a) => Decoder s a -> Decoder s (v a)
decodeVector' decode = bind decodeListLen $ \size -> VG.replicateM size decode
{-# INLINE decodeVector' #-}

encodeNamedList :: (Encodable a, HasCallStack) => [(Text, a)] -> Either Text C.Encoding
encodeNamedList ts = appendCallStack $ let ?callStack = freezeCallStack emptyCallStack in a
  where a :: HasCallStack => Either Text C.Encoding
        a = encode <$>
          mapErrors (\(n,t) -> fmap (n,) . left (("Table " <> n <> ":\n") <>) $ ensureEncodable t) ts

encodeNamedListJ :: (ToJsonAble a, HasCallStack) => [(Text, a)] -> Either Text LByteString
encodeNamedListJ ts = appendCallStack $ let ?callStack = freezeCallStack emptyCallStack in a
  where a :: HasCallStack => Either Text LByteString
        a = A.encode <$>
          mapErrors (\(n,t) -> fmap (n,) . left (("Table " <> n <> ":\n") <>) $ ensureToJson t) ts

data DiagnosticTable = DiagnosticTable (Vector (Symbol, Text, Term))
-- data D = D (Vector Symbol) (Vector TableCol)
  deriving Show

diagDeserialiseFromHex :: Text -> IO ()
diagDeserialiseFromHex = putStrLn . unlines . tableDeserialiseErrors . fromHex

-- λ> diagDeserialiseFromHex "828261618282616164636f6c3282826a4d61796265205465787482f97e0063617364826a4d61796265205465787482f97e006361736482667461626c65328282616164636f6c3282826a4d61796265205465787482f97e0063617364826a4d61796265205465787482f97e0063617364"
-- In table 'a': Column 'a' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN
-- In table 'a': Column 'col2' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN
-- In table 'table2': Column 'a' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN
-- In table 'table2': Column 'col2' of type 'Maybe Text': decodeString: unexpected token TkFloat16 NaN

tableDeserialise :: LByteString -> Either Text [(Text, Table)]
tableDeserialise bs = either (\e -> Left $ unlines $ [toS $ show e,"Diagnostics:"]
                               <>  tableDeserialiseErrors bs)
  (pure . fmap2 fromDecodeOnly) $ deserialiseOrFail bs

-- | this function is used when there are deserialisation errors to try again (into `DiagnosticTable`s)
-- via decoding from intermediate CBOR Term representation, which gives much more error information.
tableDeserialiseErrors :: LByteString -> [Text]
tableDeserialiseErrors = either (\e -> ["Error parsing CBOR in tableDeserialiseErrors: " <> toS (show e)])
  (execWriter . mapM_ @[] g) . deserialiseOrFail
  where g :: (Text, DiagnosticTable) -> Writer [Text] ()
        g (tableName, DiagnosticTable cols) = void $
          mapWriter (second $ fmap (("In table '" <> tableName <> "': " :: Text ) <>)) $
          tellErrors $ f <$> cols 
        f c@(colName, colType, _) =
          (left $ \x -> "Column '" <> colName <> "' of type '" <> colType <> "': " <> toS x)
          $ fromFlatTerm decodeColPython $ toFlatTerm $ encode c
        -- f colName x = Left $ "Column '" <> colName <> "' could not be parsed. Data[:1000]:\n" <> (toS $ pShow x)

-- | this will simply encode a directly
deriving newtype instance Serialise a => Serialise (I a)

-- | this needs to be in sync with the `Decodable Table/TableCol` instance (i.e. `decodeColPython`)
instance Serialise DiagnosticTable where 
  encode = Unsafe.error "Serialise DiagnosticTable not implemented"
  decode = DiagnosticTable <$> decode
    -- where dec = decodeListLenOf 2 >> (,) <$> decode @Text <*> decodeVector' decodeTermToken

-- | these function should not be needed anymore. use `ensureEncodable` instead
-- -- | use this to get the callstack
-- serialiseTableAnd :: (Serialise a, HasCallStack) => Table -> a -> EitherLByteString
-- serialiseTableAnd t a = C.toLazyByteString $ encodeListLen 2 <> encodeTable t <> encode a

-- -- | use this to get the callstack
-- serialiseTablesAnd :: (Serialise a, HasCallStack) => [(Text, Table)] -> a -> LByteString
-- serialiseTablesAnd t a = C.toLazyByteString $ encodeListLen 2 <> encodeNamedList t <> encode a

instance (HashableDictComponent k, DictComponent v, Encodable k, Encodable v)
  => Encodable (Dict k v) where
  encodeWithError d = liftErrors2 (\k v -> encodeListLen 2 <> k <> v)
                      (encodeWithError $ key d) $ encodeWithError $ value d

instance (HashableDictComponent k, DictComponent v, Decodable k, Decodable v)
  => Decodable (Dict k v) where
  decode' = decodeListLenOf 2 >> do
    k <- decode'
    v <- decode'
    runHorFail $ dict k v

instance (HashableDictComponent k, DictComponent v, Serialise k, Serialise v)
  => Serialise (Dict k v) where 
  encode d = encodeListLen 2 <> encode (key d) <> encode (value d)
  decode = decodeListLenOf 2 >> do
    k <- decode
    v <- decode
    runHorFail $ dict k v


instance Encodable Table where 
  encodeWithError t = appendCallStack . left (\e -> e <> "\n\n" <> showMetaWithDimensions t)
    . encodeWithError . zipDictWith NamedWrappedDyn $ flipTable t


data NamedWrappedDyn f = NamedWrappedDyn Symbol (WrappedDyn f)

namedWrappedDynToPair :: NamedWrappedDyn f -> (Symbol, WrappedDyn f)
namedWrappedDynToPair (NamedWrappedDyn a b) = (a,b)

instance EncodableContainer f => Encodable (NamedWrappedDyn f) where
  encodeWithError = fmap fst . encodeCol

instance Decodable (NamedWrappedDyn Vector) where
  decode' = decodeColPython

instance Decodable v => Decodable (Maybe v) where
  decode' = fromDecodeOnly <$> decode

instance Encodable v => Encodable (Maybe v) where
  encodeWithError = fmap encode . traverse ensureEncodable

instance Decodable v => Decodable (Vector v) where
  decode' = decodeVector' decode'

instance Encodable v => Encodable (Vector v) where
  encodeWithError = encodeVectorM' encodeWithError

instance Encodable v => Encodable [v] where
  encodeWithError = encodeVectorM' encodeWithError . toVector

instance Decodable v => Decodable [v] where
  decode' = V.toList <$> decodeVector' decode'

instance (Encodable a, Encodable b)  => Encodable (a,b) where
  encodeWithError (a,b) = encode <$> liftErrors2 (,) (ensureEncodable a) (ensureEncodable b)

instance (Decodable a, Decodable b)  => Decodable (a,b) where
  decode' = (fromDecodeOnly *** fromDecodeOnly) <$> decode

instance (Encodable a, Encodable b)  => Encodable (Either a b) where
  encodeWithError  = fmap encode . either (fmap Left . ensureEncodable) (fmap Right . ensureEncodable)

instance (Decodable a, Decodable b)  => Decodable (Either a b) where
  decode' = bimap fromDecodeOnly fromDecodeOnly <$> decode

instance Encodable Text
instance Decodable Text

instance (TableRowT r, AllFields r Serialise) => Encodable (TypedTable r)
instance (TableRowT r, AllFields r Serialise) => Decodable (TypedTable r)

instance Decodable Table where
  decode' = runHorFail . table @Vector . fmap namedWrappedDynToPair =<< decode'


class (Functor f) => EncodableContainer f where
  coerceUI2 :: f (I a) -> f a

  default coerceUI2 :: UICoerce f a => f (I a) -> f a
  coerceUI2 = coerceUI

  encodeSerialiseWith :: (a -> C.Encoding) -> f a -> C.Encoding
  encodeToJson :: ToJSON a => f a -> (A.Encoding, A.Value)

instance EncodableContainer I where
  encodeSerialiseWith e = e . unI 
  encodeToJson = (A.toEncoding &&& A.toJSON) . unI

instance EncodableContainer Vector where
  encodeSerialiseWith = encodeVector'
  encodeToJson = A.toEncoding &&& A.toJSON

encodeCol :: (EncodableContainer f, HasCallStack)
  => NamedWrappedDyn f -> Either Text (C.Encoding, (A.Encoding, A.Value))
encodeCol (NamedWrappedDyn colName (WrappedDyn tr@(App con1 tr2) wd))
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Int64)     = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Int)       = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Int32)     = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Word64)    = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Word32)    = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Word8)     = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Word)      = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Text)      = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I String)    = enc1 tr $ T.pack <$> coerceUI2 wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Char)      = enc1 tr $ T.singleton <$> coerceUI2 wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Double)    = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Float)     = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Bool)      = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I C.Time)    = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I C.Day)     = enc1 tr $ C.dayToTimeMidnight <$> coerceUI2 wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I None)      = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I Void)      = encI tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(I ByteString)=
      let w = coerceUI2 wd in enc2 (withType colName tr) w $ b64 <$> w
  | Just HRefl <- tr `eqTypeRep` typeRep @(I LByteString)=
      let w = coerceUI2 wd in enc2 (withType colName tr) w $ bl64 <$> w
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Int64)         = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Int)           = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Int32)         = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Word32)        = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Word64)        = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Word8)         = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Text)          = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Double)        = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Float)         = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Bool)          = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe C.Time)        = enc1Vec tr wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe ByteString)    =
      enc2Vec (withType colName tr) wd $ b64 <$$> wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe LByteString)   =
      enc2Vec (withType colName tr) wd $ bl64 <$$> wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe String)        = enc1Vec tr $ T.pack <$$> wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe Char)          = enc1Vec tr $ T.singleton <$$> wd
  | Just HRefl <- tr `eqTypeRep` typeRep @(Maybe C.Day)         = enc1Vec tr $ C.dayToTimeMidnight <$$> wd
  | App con2 _ <- tr2
  , Just HRefl <- con1 `eqTypeRep` typeRep @Maybe
  , Just HRefl <- con2 `eqTypeRep` typeRep @Maybe         = Left $ err
    <> " (see encodeMaybePythonStyleWithoutPossibilityOfNesting)"
  | True                                                = Left err
  where 
        err = "Trying to encode column '" <> toS colName <> "' of type " <> shot tr <> ": not implemented"
        encI :: (A.ToJSON d, Serialise d, Show t, EncodableContainer f)
          => t -> f (I d) -> Either Text AllEncodings
        encI tr = enc1 tr . coerceUI2

        enc1 :: (A.ToJSON d, Serialise d, Show t, EncodableContainer f)
          => t -> f d -> Either Text AllEncodings
        enc1 tr = join $ enc2 $ withType colName tr

        enc1Vec :: (Show b, Serialise a, A.ToJSON a, EncodableContainer f)
          => b -> f (Maybe a) -> Either c AllEncodings
        enc1Vec tr = join $ enc2Vec $ withType colName tr
        
        b64 = T.decodeASCII . B64.encode -- used for JSON
        bl64 = TL.decodeASCII . BL64.encode

enc2 :: (A.ToJSON d, Serialise b, EncodableContainer f)
  => (C.Encoding -> C.Encoding) -> f b -> f d -> Either Text AllEncodings
enc2 wrap wd wdJ = Right (wrap $ encodeSerialiseWith encode wd, encodeToJson wdJ)


withType :: Show a => Symbol -> a -> C.Encoding -> C.Encoding
withType colName tr x = encodeListLen 3 <> encode colName <> encode (show tr) <> x

-- | this is differen from the upstream instance:
-- https://hackage.haskell.org/package/serialise-0.2.6.1/docs/src/Codec.Serialise.Class.html#line-843
-- 
-- encode Nothing  = encodeListLen 0
-- encode (Just x) = encodeListLen 1 <> encode x
encodeMaybePythonStyleWithoutPossibilityOfNesting :: Serialise a => Maybe a -> Encoding
encodeMaybePythonStyleWithoutPossibilityOfNesting = maybe encodeNull encode

enc2Vec :: (Serialise a, A.ToJSON b, EncodableContainer f)
  => (C.Encoding -> C.Encoding) -> f (Maybe a) -> f (Maybe b) -> Either c AllEncodings
enc2Vec wrap wd wdJ = Right
  (wrap $ encodeSerialiseWith encodeMaybePythonStyleWithoutPossibilityOfNesting wd
  ,encodeToJson wdJ)


type AllEncodings = (C.Encoding, (A.Encoding, A.Value))

decodeVectorPythonMaybe :: (Serialise a, VG.Vector v (Maybe a)) => Decoder s (v (Maybe a))
decodeVectorPythonMaybe = decodeVector' $ do tkty <- peekTokenType
                                             case tkty of TypeNull -> Nothing <$ decodeNull
                                                          _        -> Just <$> decode

-- | this needs to be in sync with the `Serialise DiagnosticTable` instance 
decodeColPython :: HasCallStack => Decoder s (NamedWrappedDyn Vector)
decodeColPython = decodeListLenOf 3 >> do
  colName <- decode @Text
  fmap (NamedWrappedDyn colName) $ decode @Text >>= \case
    "I Int64"             -> fI @Int64            decode
    "I Int32"             -> fI @Int32            decode
    "I Int"               -> fI @Int              decode
    "I Word64"            -> fI @Word64           decode
    "I Word32"            -> fI @Word32           decode
    "I Word8"             -> fI @Word8            decode
    "I Word"              -> fI @Word             decode
    "I Bool"              -> fI @Bool             decode
    "I Text"              -> fI @Text             decode
    "I Double"            -> fI @Double           decode
    "I Float"             -> fI @Float            decode
    "I Time"              -> fI @C.Time           decode
    "I None"              -> fI @None             decode
    "I Void"              -> fI @Void             decode
    "I ByteString"        -> fI @ByteString       decode
    "I LByteString"       -> fI @LByteString      decode
    "I Char"              -> fI @Char             decode
    "I [Char]"            -> fI $ fmap2 T.unpack  decode
    "I Day"               -> fI $ fmap2 C.timeToDayTruncate decode
    "Maybe Int64"         -> fM $ decodeVectorPythonMaybe @Int64
    "Maybe Int32"         -> fM $ decodeVectorPythonMaybe @Int32
    "Maybe Int"           -> fM $ decodeVectorPythonMaybe @Int
    "Maybe Word32"        -> fM $ decodeVectorPythonMaybe @Word32
    "Maybe Word64"        -> fM $ decodeVectorPythonMaybe @Word64
    "Maybe Word8"         -> fM $ decodeVectorPythonMaybe @Word8
    "Maybe Word"          -> fM $ decodeVectorPythonMaybe @Word
    "Maybe Bool"          -> fM $ decodeVectorPythonMaybe @Bool
    "Maybe Text"          -> fM $ decodeVectorPythonMaybe @Text
    "Maybe Double"        -> fM $ decodeVectorPythonMaybe @Double
    "Maybe Float"         -> fM $ decodeVectorPythonMaybe @Float
    "Maybe Time"          -> fM $ decodeVectorPythonMaybe @C.Time
    "Maybe ByteString"    -> fM $ decodeVectorPythonMaybe @ByteString
    "Maybe LByteString"   -> fM $ decodeVectorPythonMaybe @LByteString
    "Maybe Char"          -> fM $ decodeVectorPythonMaybe @Char
    "Maybe [Char]"        -> fM $ fmap3 T.unpack                  $ decodeVectorPythonMaybe
    "Maybe Day"           -> fM $ fmap3 C.timeToDayTruncate       $ decodeVectorPythonMaybe
    colType               -> fail
              $ "decodeColPython not implemented for column " <> toS colName <> " of type " <> toS colType
  where fI :: (Wrappable a) => Decoder s (V.Vector a) -> Decoder s TableCol
        fI = fmap toWrappedDynI
        fM :: (Wrappable a) => Decoder s (f (Maybe a)) -> Decoder s (WrappedDyn f)
        fM = fmap toWrappedDyn

-- | generated with:
-- 
-- y=cbor2.dumps([["c1", ["IntCol",[1,2]]], ["c2", ["DoubleCol", [1.2, 10.29387429837239487]]], ["d", ["TextCol",["asd",""]]]])
-- print(y.hex())
--
-- result: 
-- 
-- Columns: ["c1","c2","d"]
-- Values: [[1,2],[1.2,10.293874298372394],["asd",""]]
example = do
  -- let hex = "828566496e74436f6c67426f6f6c436f6c6754657874436f6c69446f75626c65436f6c6c54696d657374616d70436f6c858266496e74436f6c8201028267426f6f6c436f6c82f5f4826754657874436f6c826273646279698269446f75626c65436f6c82fb3ff4cccccccccccdfb400199999999999a826c54696d657374616d70436f6c820102" 
  let hex = "828566496e74436f6c67426f6f6c436f6c6754657874436f6c69446f75626c65436f6c6c54696d657374616d70436f6c858266496e74436f6c82010a8267426f6f6c436f6c82f5f4826754657874436f6c826273646279698269446f75626c65436f6c82fb3ff4cccccccccccdf97e00826c54696d657374616d70436f6c82010a" 
  -- let hex = "828466496e74436f6c67426f6f6c436f6c6754657874436f6c69446f75626c65436f6c848266496e74436f6c8201028267426f6f6c436f6c82f5f4826754657874436f6c826273646279698269446f75626c65436f6c82fb3ff4cccccccccccdfb400199999999999a" 
  printDiagnostic hex
  -- let t = deserialise $ fromHex hex :: Table
  -- print t
  -- print $ select [ #k <. fmap (Chronos.encodeIso8601 . Chronos.timeToDatetime) "TimestampCol"] t
  -- pure t


fromHex :: Text -> LByteString
fromHex = toS . fromJust . H.decodeHex


pythonModule :: IO FilePath
pythonModule = getDataFileName "python/HoffSerialise.py"

instance (TableRowT r, AllFields r Serialise) => Serialise (TypedTable r) where
  encode  t = encodeListLen (2 * fromIntegral (length cols)) <> foldMap (\(n,e) -> encode n <> e) cols
    where cols = R.toList . R.cmap (Proxy @Serialise) (R.K . encode) $ columnRecord t
          cols :: [(String, Encoding)]

  decode = runHWith (fail . show) pure . typedTable =<< do
    decodeListLenOf (2 * length (R.collapse fields))
    R.cmapM (Proxy @Serialise) (\(K name) -> dec name) fields
    where dec :: Serialise a => String -> Decoder s (Vector a)
          dec name = do
            nameDec <- decode
            if name /= nameDec then fail $ "Expected column '" <> name <> "', got '" <> nameDec <> "'"
              else decode
          fields = R.reifyKnownFields Proxy
            
devmain = either (Unsafe.error . toS) printDiagnostic' $ ensureEncodable t1
  where t1 = unsafeH $ #a <# [100,1,2,3::Int64] // #b <# [1,10,20,30::Int64] // #c <# [0,10,20,30::Int64]

{- better: 
class A a where
  enc :: a -> String

instance {-# OVERLAPPABLE #-} A a where
  enc = const "arsiet"

instance {-# OVERLAPS #-} A Int where
  enc = const "a"

-}

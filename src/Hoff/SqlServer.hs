{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE DeriveAnyClass #-}

module Hoff.SqlServer where 

import Chronos
import Data.Text.Encoding as T
import Data.Text.Encoding.Error as T
import Data.Time.LocalTime (ZonedTime(..))
import Data.Vector (Vector)
import Database.ODBC.SQLServer as S
import Foreign hiding (void)
import Hoff.H
import Hoff.HQuery ()
import Hoff.Stream
import Hoff.Table
import Yahp hiding (bind)

-- | if `AssumeUtf8Char=True` the function `querySqlServer` will decode `varchar` columns (which are
-- basically raw bytestrings) as Utf8
--
-- see also Documentation of type `Value` in Database.ODBC.Internal
-- 
-- to get expected behavior:
-- BEFORE any tables are created: ALTER DATABASE db1 COLLATE  Latin1_General_100_CI_AI_SC_UTF8
newtype AssumeUtf8Char = AssumeUtf8Char Bool deriving newtype IsBool

querySqlServer :: AssumeUtf8Char -> Connection -> Query -> IO Table
querySqlServer utf8 conn q = finalizeTable (vectorToTableColumn utf8) <=< stream conn q step
  $ initState . fromList . fmap columnName
  where step state row = fmap Continue $ doStep state $ \store -> zipWithM_ store [0 ..] row

vectorToTableColumn :: AssumeUtf8Char -> Symbol -> Vector Value -> IO TableCol
vectorToTableColumn utf8 colname = vectorToTableCol (throwIO . SqlServerError) handle colname
  where handle :: (forall a . Int -> String -> String -> IO a) -> Value -> Maybe (Vector Value -> IO TableCol)
        handle err v = case v of
          TextValue    _ -> Just $ wrap $ \i -> \case
            TextValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y
          BinaryValue    _ -> Just $ wrap $ \i -> \case
            BinaryValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y
          ByteStringValue    _ -> Just $ if isTrue utf8 then
            wrap $ \i -> \case
            ByteStringValue  t -> pure2 $ decodeUtf8With lenientDecode t
            NullValue      -> pure Nothing
            y -> err2 i y
            else 
            wrap $ \i -> \case
            ByteStringValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y
          BoolValue    _ -> Just $ wrap (\i -> \case
            BoolValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y)
          DoubleValue    _ -> Just $ wrap (\i -> \case
            DoubleValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y)
          FloatValue    _ -> Just $ wrap (\i -> \case
            FloatValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y)
          IntValue    _ -> Just $ wrap (\i -> \case
            IntValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y)
          ByteValue    _ -> Just $ wrap (\i -> \case
            ByteValue  t -> pure2 t
            NullValue      -> pure Nothing
            y -> err2 i y)
          DayValue    _ -> Just $ wrap (\i -> \case
            DayValue  t -> pure2 $ fromBaseDay t
            NullValue      -> pure Nothing
            y -> err2 i y)
          TimeOfDayValue    _ -> Just $ wrap (\i -> \case
            TimeOfDayValue  t -> pure2 $ fromBaseTimeOfDay t
            NullValue      -> pure Nothing
            y -> err2 i y)
          LocalTimeValue    _ -> Just $ wrap (\i -> \case
            LocalTimeValue  t -> pure2 $ fromBaseLocalTime t
            NullValue      -> pure Nothing
            y -> err2 i y)
          NullValue         -> Nothing
          ZonedTimeValue _ _ -> Just $ wrap (\i -> \case
            ZonedTimeValue lt tz -> pure2 $ ZonedTime lt tz
            NullValue      -> pure Nothing
            y -> err2 i y)
          where err2 i = err i (Yahp.take 10 $ show v) . Yahp.take 10 . show
                err2 :: Int -> Value -> IO a

deriving instance Generic ZonedTime
deriving instance Hashable ZonedTime
deriving instance Ord ZonedTime
deriving instance Eq ZonedTime
deriving newtype instance Hashable Binary
deriving instance Generic TimeOfDay
deriving instance Hashable TimeOfDay
  

devmain = do
  -- dsn <- toS <$> getEnv "TEST_DSN"
  -- nix-build -E "with import <nixpkgs> {}; unixODBCDrivers.msodbcsql17.override { openssl = openssl_1_1; }"
  let dsn = "DRIVER=/nix/store/y6j76h30y5g2f6p8vx3xd4xcqv836znj-msodbcsql17-17.7.1.1-1/lib/libmsodbcsql-17.7.so.1.1;server=localhost;Authentication=SqlPassword;UID=sa;PWD=asd@@@ASD123;Encrypt=No;database=db1"
  conn <- connect dsn
  res <- querySqlServer (AssumeUtf8Char True) conn
    "select * from (values(1,N'\4130'),(10,N'a')) x(a,b) where 1=1 order by b DESC"
  print $ meta res
  print res
  res <- querySqlServer (AssumeUtf8Char True) conn "select * from marketdata;"
  print $ meta res
  print res

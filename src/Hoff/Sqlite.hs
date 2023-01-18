
module Hoff.Sqlite
  (module Hoff.Sqlite
  ,Connection
  ,Query
  ,ToRow(..)) where 

import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as M
import           Database.SQLite.Simple as S (Connection, Query, ToRow, bind, Statement(..), withStatement, toRow, open)
import           Database.SQLite3 as D hiding (open)
import           Hoff.Dict
import           Hoff.H
import           Hoff.HQuery.Expressions ()
import           Hoff.Table
import           Yahp hiding (bind)

  
-- | A version of `querySqlite` which does not perform parameter substitution.
querySqlite_ :: Connection -> Query -> IO Table
querySqlite_ conn query = withStatement conn query readTable

-- | Execute the query and collect results in a Hoff table
querySqlite :: (ToRow params) => Connection -> Query -> params -> IO Table
querySqlite conn template params = withStatement conn template $
  \s -> S.bind s (toRow params) >> readTable s

chunksize = 100000 -- 800kb

-- | TODO: port this to Hoff.Stream
readColumnVectors :: S.Statement -> IO (Symbols, Vector (Vector SQLData))
readColumnVectors (S.Statement s) = do
  ncols <- columnCount s
  let getName c = maybe (throwIO $ userError $ "NULL pointer in SQlite Statement " <> show c) pure =<< columnName s c
      ncolsi = fromIntegral ncols
      colIndexes = V.enumFromTo 0 (ncols-1)
      write index = V.zipWithM_ (\i m -> M.unsafeWrite m index =<< column s i) colIndexes
      newCols = V.replicateM ncolsi $ M.unsafeNew chunksize
      doStep index collect currentColVector = step s >>= \case
        Row -> if index == chunksize then do
          nextColVector <- newCols
          write 0 nextColVector
          frozen <- mapM V.unsafeFreeze currentColVector
          doStep 1 (\a -> collect frozen . (a:)) nextColVector
          else do
          write index currentColVector
          doStep (index+1) collect currentColVector
        Done -> flip collect [] <$> mapM (V.unsafeFreeze . M.take index) currentColVector
  chunks <- doStep 0 (:|) =<< newCols
  -- print chunks
  (,concatChunks chunks) <$> mapM getName colIndexes

concatChunks :: NonEmpty (Vector (Vector a)) -> Vector (Vector a)
concatChunks (x :| []) = x
concatChunks (x :| xs) = V.generate (length x) (\i -> V.concat $ (V.! i) <$> (x:xs))

readTable :: S.Statement -> IO Table
readTable s = do
  (colNames, colVectors) <- readColumnVectors s
  runHinIO . (\x -> table2 =<< dict colNames x) =<< V.zipWithM vectorToTableColumn colNames colVectors

vectorToTableColumn :: Symbol -> Vector SQLData -> IO TableCol
vectorToTableColumn colname = vectorToTableCol (throwIO . SqliteError) handle colname
  where handle :: (forall a . Int -> String -> String -> IO a) -> SQLData -> Maybe (Vector SQLData -> IO TableCol)
        handle err v = case v of
          SQLInteger    _ -> Just $ wrap (\i -> \case
            SQLInteger  t -> pure2 t
            SQLNull       -> pure Nothing
            y -> err2 i y)
          SQLFloat      _ -> Just $ wrap (\i -> \case
            SQLFloat    t -> pure2 t
            SQLNull       -> pure Nothing
            y -> err2 i y)
          SQLText       _ -> Just $ wrap (\i -> \case
            SQLText     t -> pure2 t
            SQLNull       -> pure Nothing
            y -> err2 i y)
          SQLBlob       _ -> Just $ wrap (\i -> \case
            SQLBlob     t -> pure2 t
            SQLNull       -> pure Nothing
            y -> err2 i y)
          SQLNull         -> Nothing
          where err2 i = err i (Yahp.take 10 $ show v) . Yahp.take 10 . show
                err2 :: Int -> SQLData -> IO a

devmain = do
  conn <- open ":memory:"
  res <- querySqlite_ conn
    "select * from (values(1,2,null,null,null,3,null),(null,3,2.3,\"asd\",null, false, x'6249'))"
  print $ meta res
  print res

  let t1 = tcF #column1 [Just @Int64 1, Nothing] //
        tc  #column2 [2 :: Int64 ,3] //
        tcF #column3 [Nothing, Just @Double 2.3] //
        tcF #column4 [Nothing, Just @Text "asd"] //
        tc  #column5 [None (), None ()] //
        tc  #column6 [3,0::Int64] //
        tcF #column7 [Nothing, Just @ByteString "bI"]
  print t1



{-# LANGUAGE TemplateHaskell #-}
module Hoff.Stream where 

import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as M
import           Hoff.Dict
import           Hoff.H
import           Control.Lens ((%~))
import           Hoff.HQuery.Expressions ()
import           Hoff.Table
import           Yahp hiding (bind)

type Prepend a = a -> [a] -> NonEmpty a

data StreamState a = StreamState        { sIndex        :: !Int
                                        , sChunks       :: !(Prepend (Vector (Vector a)))
                                        , sColumnNames  :: Symbols
                                        , sCurrentChunk :: !(Vector (M.IOVector a))
                                        }

makeLensesWithSuffix ''StreamState

finalizeTable :: (Symbol -> Vector a -> IO TableCol) -> StreamState a -> IO Table
finalizeTable vectorToTableColumn StreamState{..} = do
  v <- mapM (V.unsafeFreeze . M.take sIndex) sCurrentChunk
  liftIO $ runHinIO . (\x -> table2 =<< dict sColumnNames x) =<<
    V.zipWithM vectorToTableColumn sColumnNames (concatChunks $ sChunks v [])

concatChunks :: NonEmpty (Vector (Vector a)) -> Vector (Vector a)
concatChunks (x :| []) = x
concatChunks (x :| xs) = V.generate (length x) (\i -> V.concat $ (V.! i) <$> (x:xs))

chunksize :: Int
-- chunksize = 1
chunksize = 100000 -- 800kb of Memory

initState :: Symbols -> IO (StreamState a)
initState colNames = StreamState 0 (:|) colNames <$> getNewChunk (length colNames)

getNewChunk :: Int -> IO (Vector (M.IOVector a))
getNewChunk ncols = V.replicateM ncols $ M.unsafeNew chunksize

doStep :: StreamState a -> ((Int -> a -> IO ()) -> IO ()) -> IO (StreamState a)
doStep s withColumnValueConsumer = -- fmap (\x -> trace (show (sIndex x, length $ sChunks x undefined [])) x) $
  if sIndex s == chunksize then do
    frozen <- mapM V.unsafeFreeze $ sCurrentChunk s
    nextChunk <- newChunk
    writeAndGo $ StreamState 0 (\x -> sChunks s frozen . (x:)) (sColumnNames s) nextChunk
  else
    writeAndGo s
  where writeAndGo s2 = slap (s2 & sIndex_ %~ succ) $ withColumnValueConsumer $ 
          \i -> M.unsafeWrite (sCurrentChunk s2 V.! i) $ sIndex s2
        newChunk = getNewChunk $ V.length $ sCurrentChunk s

{-# LANGUAGE PolyKinds #-}
module Hoff.Utils where

import           Control.Monad.IO.Class
import qualified Data.Coerce
import           Data.SOP
import           Data.Text hiding (foldl')
import           Data.Time
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified System.Clock as C
import           Text.Printf
import           TextShow as TS hiding (fromString)
import           Yahp hiding (get, group, groupBy, (&), TypeRep, typeRep)

time :: MonadIO m => Text -> m t -> m t
time name a = do
  let gt = liftIO $ C.getTime C.Monotonic
  start <- gt
  v <- a
  end <- gt
  let diff = (fromInteger $ C.toNanoSecs (end - start)) / (10^(9::Integer))
  timestamp <- liftIO getCurrentTime
  v <$ liftIO (printf "[%s]: %0.6f sec for %s\n"
               (formatTime defaultTimeLocale  "%Y-%m-%d %H:%M:%S%3Q" timestamp)
                (diff :: Double) name)


type ICoerce f a = Data.Coerce.Coercible (f a) (f (I a))
type UICoerce f a = Data.Coerce.Coercible (f (I a)) (f a)

coerceI :: forall a f . ICoerce f a => f a -> f (I a)
coerceI = Data.Coerce.coerce
{-# INLINE coerceI #-}

coerceUI :: forall a f . UICoerce f a => f (I a) -> f a
coerceUI = Data.Coerce.coerce
{-# INLINE coerceUI #-}

newtype None = None () -- Python yay!
  deriving (Eq, Ord, Hashable)

instance Show None where show _ = "None"
instance TextShow None where showbPrec _ _ = fromText "None"

fromInt64 :: Num a => Int64 -> a
fromInt64 = fromIntegral
{-# INLINE fromInt64 #-}

fromDouble :: Fractional a => Double -> a
fromDouble = realToFrac
{-# INLINE fromDouble #-}


-- * helper for concatenating vectors in linear time

newtype VectorBuilder a = VectorBuilder { unVectorBuilder :: [Vector a] -> [Vector a] }

builderToVector :: VectorBuilder a -> Vector a
builderToVector = V.concat . ($ []) . unVectorBuilder

builderFromVector :: Vector a -> VectorBuilder a
builderFromVector = VectorBuilder . (:)

appendVector :: VectorBuilder a -> Vector a -> VectorBuilder a
appendVector (VectorBuilder b) a = VectorBuilder $ b . (a:)

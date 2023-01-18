{-# OPTIONS_GHC -Wno-unused-top-binds #-}
import           System.Random
import           Hoff.Vector
import qualified Data.Vector as V
import qualified Data.Text as T
import           Yahp
import           System.IO.Unsafe
import           Hoff.Iterable
import           System.TimeIt
import           Data.Coerce
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Data.Vector.Algorithms.Intro as Intro
import           Data.Vector.Algorithms.Merge as Merge
import           Data.Vector.Algorithms.Intro as Reexport (Comparison)
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Mutable as M
import           Hoff.H
import           Hoff.Vector
import qualified Prelude as Unsafe
import           Yahp hiding (group, take)


--  in GHCI, use :set +s

n = 1000000
v :: Vector Int
v = unsafePerformIO $ V.replicateM n (randomIO :: IO Int)
vs :: Vector Text
vs = showt <$> v
y = V.enumFromN (0 :: Int) n


main = void $ do
  timeItNamed "init rand                "  $ evaluate $ V.sum v
  timeItNamed "init str(rand)           "  $ evaluate $ V.sum $ T.length <$> vs
  timeItNamed "init sorted              "  $ evaluate $ V.sum y
  timeItNamed "sort rand                "  $ evaluate $ sum $ sort $ toList v
  timeItNamed "sort sorted              " $ evaluate $ sum $ sort $ toList y
  -- timeItNamed "ascV rand                "  $ evaluate $ V.sum $ ascV v
  -- timeItNamed "ascV str(rand)           "  $ print $ V.head $ ascV vs
  -- timeItNamed "ascV sorted              " $ evaluate $ V.sum $ ascV y
  -- timeItNamed "sortV flip compare rand  "  $ evaluate $ V.sum $ sortV (flip compare) v
  -- timeItNamed "sortByWithUnsafe2 rand    "  $ print $ V.sum $ sortByWithUnsafe2 v compare v
  -- timeItNamed "sortByWithUnsafe2 str(rand)"  $ print $ V.head $ sortByWithUnsafe2 vs compare vs
  -- timeItNamed "sortByWithUnsafe2 sorted    "  $ print $ V.sum $ sortByWithUnsafe2 y compare y
  -- timeItNamed "argsort rand             "  $ print $ V.sum  $ argsort v compare v
  -- timeItNamed "argsort str(rand)        "  $ print $ V.head $ argsort vs compare vs
  -- timeItNamed "argsort sorted           "  $ print $ V.sum  $ argsort y compare y
  -- timeItNamed "sortByWithUnsafe3 rand    "  $ print $ V.sum $ sortByWithUnsafe3 v compare v
  -- timeItNamed "sortByWithUnsafe3 str(rand)"  $ print $ V.head $ sortByWithUnsafe3 vs compare vs
  -- timeItNamed "sortByWithUnsafe3 sorted    "  $ print $ V.sum $ sortByWithUnsafe3 y compare y


  
devmain = main

data P a = P a Int
  deriving (Eq, Ord)

sortByWithUnsafe2 :: (Ord x, Iterable v) => Vector x -> Comparison x -> v -> Vector Int
sortByWithUnsafe2 with by v = fmap fst  $ V.create $ do
  mv <- V.thaw $ V.indexed with
  mv <$ Intro.sortBy (\x y -> by (snd x) (snd y)) mv
  -- mv <$ Merge.sortBy compare mv

argsort :: (Ord x, Iterable v) => Vector x -> Comparison x -> v -> Vector Int
argsort with by v = V.create $ do
  mv <- M.generate (length with) id
  mv <$ Intro.sortBy (comparing (with V.!)) mv
  -- mv <$ Merge.sortBy compare mv
{-# INLINABLE argsort #-}

-- sortByWithUnsafe2 :: (Ord x, Iterable v) => Vector x -> Comparison x -> v -> Vector x
-- sortByWithUnsafe2 with by v = fmap (\(P x _) -> x) $ V.create $ do
--   mv <- V.thaw $ V.imap (flip P) with
--   mv <$ Intro.sortBy compare mv
--   -- mv <$ Merge.sortBy compare mv
-- {-# INLINABLE sortByWithUnsafe2 #-}

sortByWithUnsafe3 :: (Ord x, Iterable v) => Vector x -> Comparison x -> v -> Vector Int
sortByWithUnsafe3 with by v = fmap fst  $ V.create $ do
  mv <- V.thaw $ V.indexed with
  mv <$ Intro.sortBy (\(_,x) (_,y) -> by x y) mv


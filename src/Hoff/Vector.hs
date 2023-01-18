{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
module Hoff.Vector where

import qualified Data.HashMap.Internal as HI
import qualified Data.HashMap.Strict as HM
import qualified Data.HashTable.ST.Cuckoo as HT
import           Data.STRef
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Data.Vector.Algorithms (nubBy)
import           Data.Vector.Algorithms.Intro (sortBy, Comparison)
import qualified Data.Vector.Algorithms.Radix as R
-- import           Data.Vector.Fusion.Bundle ( Bundle, MBundle, lift, inplace )
import qualified Data.Vector.Fusion.Bundle as Bundle
-- import           Data.Vector.Fusion.Stream.Monadic ( Stream(..), Step(..) )
-- import qualified Data.Vector.Fusion.Stream.Monadic as S
import qualified Data.Vector.Generic as G hiding (toList)
import qualified Data.Vector.Mutable as M
import           Yahp hiding (sortBy)

instance (Hashable a) => Hashable (V.Vector a) where
  hashWithSalt salt = hashWithSalt salt . toList
  {-# INLINE hashWithSalt #-}

-- | prefers the second
unsafeBackpermuteMaybe2 :: (G.Vector v a, G.Vector v a, G.Vector v (Maybe Int) , G.Vector v Int)
  => v a -> v a -> v Int -> v (Maybe Int) -> v a
{-# INLINE unsafeBackpermuteMaybe2 #-}
unsafeBackpermuteMaybe2 v1 v2 is1 is2 = seq v1 $ seq v2
                       $ G.unstream
                       $ Bundle.unbox
                       $ Bundle.zipWith index (G.stream is1) (G.stream is2)
  where
    {-# INLINE index #-}
    index left right = maybe (G.basicUnsafeIndexM v1 left) (G.basicUnsafeIndexM v2) right

unsafeBackpermuteMaybeV :: (G.Vector v a, G.Vector v (Maybe a), G.Vector v (Maybe Int)) => v a -> v (Maybe Int) -> v (Maybe a)
{-# INLINE unsafeBackpermuteMaybeV #-}
unsafeBackpermuteMaybeV v is =  seq v
                                $ G.unstream
                                $ Bundle.unbox
                                $ Bundle.map index
                                $ G.stream is
  where
    {-# INLINE index #-}
    index = traverse $ G.basicUnsafeIndexM v

-- | conditional a b c = "if a then b else c"
conditional :: (G.Vector v a, G.Vector v Bool) => v Bool -> v a -> v a -> v a
conditional = G.zipWith3 $ \a b c -> if a then b else c
{-# INLINABLE conditional #-}


-- | returns the indices of the unique elements
distinctVIndicesSuperSlow :: Ord a => V.Vector a -> V.Vector Int
distinctVIndicesSuperSlow = fmap fst . nubBy (comparing snd) . V.indexed 
{-# INLINABLE distinctVIndicesSuperSlow #-}


-- | returns the indices of the unique elements
distinctVIndicesHm :: (Eq a, Hashable a) => V.Vector a -> V.Vector Int
distinctVIndicesHm = sortV compare  . V.fromList . HM.elems . HM.fromList . toList . fmap swap . V.reverse . V.indexed
{-# INLINABLE distinctVIndicesHm #-}

distinctVIndicesHt :: (Eq a, Hashable a) => V.Vector a -> V.Vector Int
distinctVIndicesHt v = V.create $ do
  m <- M.unsafeNew $ length v
  ht <- HT.newSized $ length v
  let handle mIdx vIdx val = do exists <- HT.lookup ht val
                                case exists of
                                  Just () -> pure mIdx
                                  _       -> do
                                    M.write m mIdx vIdx
                                    succ mIdx <$ HT.insert ht val ()
                              
  mIdx <- V.ifoldM'  handle 0 v
  pure $ M.take mIdx m
{-# INLINABLE distinctVIndicesHt #-}
  
  
distinctVIndices :: (Eq a, Hashable a) => V.Vector a -> V.Vector Int
distinctVIndices v = V.create $ do 
  m <- M.unsafeNew $ length v
  hm <- newSTRef $ HM.empty
  let handle mIdx vIdx val = do exists <- HM.lookup val <$> readSTRef hm
                                case exists of
                                  Just () -> pure mIdx
                                  _       -> do
                                    M.write m mIdx vIdx
                                    succ mIdx <$ modifySTRef' hm (HI.unsafeInsert val ())
                              
  mIdx <- V.ifoldM'  handle 0 v
  pure $ M.take mIdx m
{-# INLINABLE distinctVIndices #-}
  
distinctVBy :: (Eq a, Hashable a) => (b -> a) -> V.Vector b -> V.Vector b
distinctVBy f v = V.create $ do 
  m <- M.unsafeNew $ length v
  hm <- newSTRef $ HM.empty
  let handle mIdx val = do let valBy = f val
                           exists <- HM.lookup valBy <$> readSTRef hm
                           case exists of
                             Just () -> pure mIdx
                             _       -> do
                               M.write m mIdx val
                               succ mIdx <$ modifySTRef' hm (HI.unsafeInsert valBy ())
                              
  mIdx <- V.foldM'  handle 0 v
  pure $ M.take mIdx m
{-# INLINABLE distinctVBy #-}
  
  
distinctV :: (Eq a, Hashable a) => V.Vector a -> V.Vector a
distinctV v = V.create $ do 
  m <- M.unsafeNew $ length v
  hm <- newSTRef $ HM.empty
  let handle mIdx val = do exists <- HM.lookup val <$> readSTRef hm
                           case exists of
                             Just () -> pure mIdx
                             _       -> do
                               M.write m mIdx val
                               succ mIdx <$ modifySTRef' hm (HI.unsafeInsert val ())
                              
  mIdx <- V.foldM'  handle 0 v
  pure $ M.take mIdx m
{-# INLINABLE distinctV #-}
  
  
sortVr :: R.Radix x => V.Vector x -> V.Vector x
sortVr v = V.create $ bind (V.thaw v) $ \m -> m <$ R.sort m
{-# INLINE sortVr #-}

sortV :: Comparison x -> V.Vector x -> V.Vector x
sortV cmp v = V.create $ bind (V.thaw v) $ \m -> m <$ sortBy cmp m
{-# INLINE sortV #-}

isSortedSlow :: Comparison x -> V.Vector x -> Bool
isSortedSlow by v = V.null v || g 1 (V.head v)
  where g idx val0 = idx == V.length v || case by val0 val1 of
          GT -> False
          _ -> g (idx+1) val1
          where val1 = v V.! idx

isSorted :: Comparison t -> Vector t -> Bool
isSorted by v = V.null v || V.notElem GT (V.zipWith by v $ V.tail v)

-- | Forward fill
--
-- ffillV $ fromList [Nothing, Just 'a', Nothing, Just 'b', Nothing]
--
-- [Nothing,Just 'a',Just 'a',Just 'b',Just 'b']
-- 
ffillV :: Vector (Maybe a) -> Vector (Maybe a)
ffillV = V.postscanl (\prev -> \case { Nothing -> prev; x -> x }) Nothing
{-# INLINE ffillV #-}

-- | backward fill
-- 
-- ffillV $ fromList [Nothing, Just 'a', Nothing, Just 'b', Nothing]
--
-- [Just 'a',Just 'a',Just 'b',Just 'b',Nothing]
-- 
bfillV :: Vector (Maybe a) -> Vector (Maybe a)
bfillV = V.postscanr (\current prev -> case current of { Nothing -> prev; x -> x }) Nothing
{-# INLINE bfillV #-}

ffillDefV :: a -> Vector (Maybe a) -> Vector a
ffillDefV = V.postscanl $ \prev current -> case current of { Just c -> c; _ -> prev }
{-# INLINE ffillDefV #-}

bfillDefV :: a -> Vector (Maybe a) -> Vector a
bfillDefV = V.postscanr $ \current prev -> case current of { Just c -> c; _ -> prev }
{-# INLINE bfillDefV #-}

ffillGeneric :: (a -> Bool) -> a -> Vector a -> Vector a
ffillGeneric isNoth = V.postscanl $ \prev current -> if isNoth current then prev else current
{-# INLINE ffillGeneric #-}

bfillGeneric :: (a -> Bool) -> a -> Vector a -> Vector a
bfillGeneric isNoth = V.postscanr $ \current prev -> if isNoth current then prev else current
{-# INLINE bfillGeneric #-}

-- ffillGeneric :: (a -> Bool) -> a -> Vector a -> Vector a
-- ffillGeneric isNoth noth = G.unstream . inplace (ffillS noth isNoth) id . G.stream
-- {-# INLINE ffillGeneric #-}

-- bfillGeneric :: (a -> Bool) -> a -> Vector a -> Vector a
-- bfillGeneric isNoth noth = G.unstreamR . inplace (ffillS noth isNoth) id . G.streamR
-- {-# INLINE bfillGeneric #-}

-- #define INLINE_FUSED INLINE [1]
-- #define INLINE_INNER INLINE [0]

-- -- inspired by https://hackage.haskell.org/package/vector-stream-0.1.0.0/docs/src/Data.Stream.Monadic.html#mapM
-- ffillS :: Monad m => a -> (a -> Bool) -> Stream m a -> Stream m a
-- {-# INLINE_FUSED ffillS #-}
-- ffillS noth isNoth (Stream next0 s0) = Stream next (s0, noth)
--   where
--     {-# INLINE_INNER next #-}
--     next !(s, prev) = do
--         step <- next0 s
--         return $ case step of
--             Done                -> Done
--             Skip    s'          -> Skip         (s', prev)
--             Yield x s'          -> let y = if isNoth x then prev else x
--                                    in Yield y      (s', y)

prevV :: a -> Vector a -> Vector a
prevV d = V.cons d . V.init
{-# INLINE prevV #-}

nextV :: b -> Vector b -> Vector b
nextV d = flip V.snoc d . V.tail
{-# INLINE nextV #-}

csumV :: Num b => Vector b -> Vector b
csumV = V.scanl1' (+)
{-# INLINE csumV #-}

cproductV :: Num b => Vector b -> Vector b
cproductV = V.scanl1' (*)
{-# INLINE cproductV #-}


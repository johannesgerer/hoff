{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE UndecidableInstances #-}

module Hoff.Iterable
  (module Hoff.Iterable
  ,module Reexport
  ) where

import           Data.Coerce
import qualified Data.HashMap.Strict as HM
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Data.Vector.Algorithms.Intro as Intro
import           Data.Vector.Algorithms.Intro as Reexport (Comparison)
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Mutable as M
import           Hoff.H
import           Hoff.Vector
import qualified Prelude as Unsafe
import           Yahp hiding (group, take)


type VectorH a = H (Vector a)

newtype GroupIndex = GroupIndex { fromGroupIndex :: Int }
  deriving (Show, Num, Eq, Hashable, Ord)

newtype IterIndex = IterIndex { fromIterIndex :: Int }
  deriving (Show, Num, Eq, Hashable, Ord, Generic, NFData)

-- | vector containing for each group its vector of source indices
data Grouping = Grouping        { gOriginalLength       :: Int 
                                , gSourceIndexes        :: RawGrouping }
  deriving Show

type RawGrouping = Vector (Vector IterIndex)

type KeyVector v = Vector (KeyItem v) 

class Iterable v where
  null                  :: HasCallStack => v            -> Bool
  default null          :: (G.Vector u Int, G.Vector u i, v ~ u i) => v            -> Bool
  null = G.null
  {-# INLINE null #-}

  count                 :: HasCallStack => v            -> Int
  default count         :: (G.Vector u Int, G.Vector u i, v ~ u i) => v            -> Int
  count = G.length
  {-# INLINE count #-}

  -- | negative n means taking the last n
  take :: Int -> v -> v
  default take          :: (G.Vector u Int, G.Vector u i, v ~ u i) => Int -> v            -> v
  take n | n >= 0 = G.take n
         | True   = \v -> G.drop (G.length v + n) v
  {-# INLINABLE take #-}

  drop :: Int -> v -> v
  default drop          :: (G.Vector u Int, G.Vector u i, v ~ u i) => Int -> v            -> v
  drop = G.drop
  {-# INLINE drop #-}

  unsafeBackpermute     :: Vector IterIndex -> v -> v
  default unsafeBackpermute :: (G.Vector u Int, G.Vector u i, v ~ u i) => Vector IterIndex -> v -> v
  unsafeBackpermute k v = G.unsafeBackpermute v (V.convert (coerce k :: V.Vector Int))
  {-# INLINABLE unsafeBackpermute #-}

  reverse :: v -> v

  
class Iterable v => DistinctIterable v where
  distinct      :: HasCallStack => v -> v

distinctByM :: (Functor f, Iterable b, Hashable a) => (b -> f (Vector a)) -> b -> f b
distinctByM byM x = ffor (byM x) $ \by -> unsafeBackpermute (coerce $ distinctVIndices by) x

distinctBy :: (Iterable v, Hashable a) => (v -> Vector a) -> v -> v
distinctBy by x = unsafeBackpermute (coerce $ distinctVIndices $ by x) x

class Iterable v => DictComponent v where
  type KeyItem v :: Type
  type Nullable v :: Type

  -- | get keys from second arg that are directly comparable with the first arg's keys
  toCompatibleKeyVector :: HasCallStack => v -> v -> H (KeyVector v)
  default toCompatibleKeyVector :: (v ~ KeyVector v) => v -> v -> H (KeyVector v)
  toCompatibleKeyVector _ = pure
  {-# INLINE toCompatibleKeyVector #-}

  unsafeBackpermuteMaybe :: Vector (Maybe IterIndex) -> v -> H (Nullable v)
  default unsafeBackpermuteMaybe :: (Nullable v ~ u (Maybe i), G.Vector u (Maybe Int), G.Vector u (Maybe i), G.Vector u i, v ~ u i)
                                 => Vector (Maybe IterIndex) -> v -> H (Nullable v)
  unsafeBackpermuteMaybe k v = pure $ unsafeBackpermuteMaybeV v (V.convert (coerce k :: V.Vector (Maybe Int)))
  {-# INLINE unsafeBackpermuteMaybe #-}

  -- (!)             :: HasCallStack => v          -> Int  -> KeyItem v
  -- {-# INLINE (!) #-}
  -- default (!) :: (G.Vector u i, v ~ u i, KeyItem v ~ i, HasCallStack) => v          -> Int  -> KeyItem v
  -- (!) = (G.!)

  toUnsafeKeyVector :: v -> KeyVector v
  default toUnsafeKeyVector :: (v ~ KeyVector v) => v -> KeyVector v
  toUnsafeKeyVector = id
  {-# INLINE toUnsafeKeyVector #-}

type HashableKey v = (Eq (KeyItem v), Hashable (KeyItem v))
type HashableKeyVector a = (Eq a, Hashable a)
type HashableDictComponent v = (HashableKey v, DictComponent v)
  
instance Iterable (Vector i) where
  reverse = V.reverse
  {-# INLINE reverse #-}

instance (Eq i, Hashable i) => DistinctIterable (Vector i) where
  distinct = distinctV
  {-# INLINE distinct #-}

instance DictComponent (Vector i) where
  type KeyItem (Vector i) = i
  type Nullable (Vector i) = Vector (Maybe i)


-- * Low level

addIndexWith :: (IterIndex -> a) -> Vector c -> Vector (c, a)
addIndexWith f = V.imap $ \i b -> (b, f $ IterIndex i)
{-# INLINABLE addIndexWith #-}

buildHashMap :: (Show a, HashableDictComponent b) => (a -> a -> a) -> (IterIndex -> a) ->  b -> (HM.HashMap (KeyItem b) a)
buildHashMap g f = HM.fromListWith g . toList . addIndexWith f . toUnsafeKeyVector
{-# INLINABLE buildHashMap #-}

-- * Sorting (unstable)
-- 
-- All functions check if the object is not already sorted

sortByM :: (Functor m, Iterable v) => (v -> m (Vector x)) -> Comparison x -> v -> m v
sortByM by by2 v = ffor (by v) $ \w -> sortByUnsafe w by2 v
{-# INLINABLE sortByM #-}

sortByUnsafe :: Iterable v => Vector x -> Comparison x -> v -> v
sortByUnsafe with by v = if isSorted by with then v else
  flip unsafeBackpermute v . coerce1' @IterIndex . fmap fst $ V.create $ do
  mv <- V.thaw $ V.indexed with
  mv <$ Intro.sortBy (\(_,x) (_,y) -> by x y) mv
{-# INLINABLE sortByUnsafe #-}

sortBy :: Iterable v => (v -> Vector x) -> Comparison x -> v -> v
sortBy with by v = sortByUnsafe (with v) by v
{-# INLINABLE sortBy #-}

 
ascV :: Ord x => Vector x -> Vector x
ascV = sortV compare
{-# INLINE ascV #-}

descV :: Ord x => Vector x -> Vector x
descV = sortV $ flip compare
{-# INLINE descV #-}

-- * Grouping


-- | takes a pre-grouped vector and returns the a vector of groups
--
-- unsafe, because the iterable could have fewer elements than expected by the grouping, i.e. silent
-- index access out of bounds
unsafeApplyGrouping :: (Iterable b) => Grouping -> b -> Vector b
unsafeApplyGrouping g v = flip unsafeBackpermute v <$> gSourceIndexes g
{-# INLINE unsafeApplyGrouping #-}

applyGrouping :: (HasCallStack, Iterable b) => Grouping -> Text -> b -> VectorH b
applyGrouping g msg v = unsafeApplyGrouping g v <$ guardH
  (CountMismatch $ toS msg <> ":Trying to group vector with length " <> show (count v) <> ", expected: "
   <> show (gOriginalLength g)) (gOriginalLength g == count v)
 
-- | returns the vector of unique elements and a vector containing the indices for each unique element
-- preserves in-group order (if HM.fromListWith and therefore buildHashMap preserve order)
getGroupsAndGrouping :: HashableDictComponent t => t -> (t, Grouping)
getGroupsAndGrouping vals = (unsafeBackpermute (Unsafe.head <$> idxs) vals
                            , Grouping (count vals) $ V.reverse . V.fromList <$> idxs)
  where idxs = V.fromList . toList $ groupIndices vals
{-# INLINABLE getGroupsAndGrouping #-}


-- | return a vector of pre-grouped shape containing indices pointing to concatenated grouped columns
groupInversionIndices :: Grouping -> Vector IterIndex
groupInversionIndices = coerce . sortByIterIndex . V.indexed . V.concatMap id . gSourceIndexes

sortByIterIndex :: Vector (a, IterIndex) -> Vector a
sortByIterIndex pairs = V.create $ do
  res <- M.unsafeNew $ V.length pairs
  res <$ mapM (\(gi, IterIndex i) -> M.write res i gi) pairs
{-# INLINABLE sortByIterIndex #-}

-- | return a vector of pre-grouped shape containing the corresponding group indices
broadcastGroupIndex :: Grouping -> Vector GroupIndex
broadcastGroupIndex = coerce . sortByIterIndex . V.concatMap sequence . V.indexed . gSourceIndexes

-- | takes a grouping and vector of group values and returns a vector of pre-grouped size with
-- corresponding group values
broadcastGroupValue :: Iterable t => Grouping -> t -> t
broadcastGroupValue gs = unsafeBackpermute $ coerce $ broadcastGroupIndex gs
{-# INLINABLE broadcastGroupValue #-}
  
-- | returns map from values to the list of indices where they appear
-- preserves in-group order (if HM.fromListWith and therefore buildHashMap preserve order)
groupIndices :: (HashableDictComponent v) => v -> HM.HashMap (KeyItem v) [IterIndex]
groupIndices = buildHashMap collect pure
  where collect new old = case new of {[n] -> n:old; _ -> Unsafe.error "Hoff.Iterable.groupIndices.neverever"}
{-# INLINABLE groupIndices #-}

-- type ToVector f a = (Typeable f, Typeable a, Foldable f)

class ToVector f where
  toVector :: f a -> Vector a

instance Foldable f => ToVector f where toVector = V.fromList . toList
instance {-# OVERLAPS #-}ToVector Vector where toVector = id

-- toVector :: forall a f . ToVector f a => f a -> Vector a
-- toVector f | Just HRefl <- R.typeOf f `eqTypeRep` R.typeRep @(Vector a) = f
--            | True                                                       = V.fromList $ toList f


-- * Where

data WhereResult = UnsafeEverything Int -- ^ length
                 | UnsafeEmpty Int -- ^ length
                 | UnsafeProperSubset (Vector Bool) -- ^ mask
                   (Vector IterIndex) -- ^ selected indices subset
  deriving (Show, Eq, Ord)

invert :: WhereResult -> WhereResult
invert = \case
  UnsafeEverything n          -> UnsafeEmpty n
  UnsafeEmpty      n          -> UnsafeEverything n
  UnsafeProperSubset m _      -> properSubset $ not <$> m

properSubset :: Vector Bool -> WhereResult
properSubset m = UnsafeProperSubset m $ V.map IterIndex $ V.findIndices id m

fromMask :: Vector Bool -> WhereResult
fromMask mask   | V.and mask            = UnsafeEverything $ count mask
                | not (V.or mask)       = UnsafeEmpty $ count mask
                | True                  = properSubset mask

withWhereResult :: (HasCallStack, Iterable t) => (t -> H a) -> (t -> H a)
  -> (Vector Bool -> Vector IterIndex -> t -> H a) -> t -> WhereResult -> H a
withWhereResult everything empty subset i = \case
  UnsafeEverything n          -> req n $ everything i
  UnsafeEmpty      n          -> req n $ empty i
  UnsafeProperSubset mask idcs-> req (count mask) $ subset mask idcs i
  where req n x = if count i == n then x else throwH
          $ CountMismatch $ "Iterable length " <> show (count i) <> " ≠ WhereResult length " <> show n 
{-# INLINABLE withWhereResult #-}

applyWhereResult :: HasCallStack => Iterable p => p -> WhereResult -> H p
applyWhereResult = withWhereResult pure (pure . take 0) (\_ idcs -> pure . unsafeBackpermute idcs)
{-# INLINE applyWhereResult #-}


replicateSameLength :: Iterable i => i -> v -> Vector v
replicateSameLength = V.replicate . count
{-# INLINE replicateSameLength #-}

replicateNothing :: Iterable a1 => a1 -> Vector (Maybe a2)
replicateNothing = flip replicateSameLength Nothing
{-# INLINE replicateNothing #-}

-- | apply the smaller argument first
applySmallerFirst :: Iterable a => (a -> a -> c) -> a -> a -> c 
applySmallerFirst f a b | count a <= count b  = f a b
                        | True                = f b a
{-# INLINE applySmallerFirst #-}

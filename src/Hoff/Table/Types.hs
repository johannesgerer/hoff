{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module Hoff.Table.Types
  (module Hoff.Table.Types
  ,module Reexport
  ) where

import           Control.Lens
import           Data.Coerce
import           Data.Functor.Classes
import           Data.Functor.Classes as Reexport
import qualified Data.HashTable.ST.Cuckoo as HT
import           Data.Hashable
import           Data.Hashable.Lifted (Hashable1(..), hashWithSalt1)
import           Data.SOP hiding (S)
import           Data.String
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Show
import           Hoff.Utils
import qualified Prelude
import           TextShow hiding (fromString)
import           Type.Reflection as R
import           Unsafe.Coerce ( unsafeCoerce )
import           Yahp as P hiding (get, group, groupBy, (&), TypeRep, typeRep, Hashable, hashWithSalt, (:.:), null, (<|>))

-- TODO: do not do unnecessary bound checks


class (Eq a, Typeable a, Hashable a, AtomShow a, Ord a) => Wrappable a
instance (Eq a, Typeable a, Hashable a, AtomShow a, Ord a) => Wrappable a
  
type WrapCoerce f g a = Data.Coerce.Coercible (f (g a)) (f (WrapInstances g a))
type UnWrapCoerce f g a = Data.Coerce.Coercible (f (WrapInstances g a)) (f (g a))
type Wrappable1 a = (Eq1 a, Hashable1 a, AtomShow1 a, Typeable a, Ord1 a)
type Wrappable2 g a = (Wrappable a, Wrappable1 g)
type WrapInstancesClass a = (Eq a, Hashable a, Ord a)

newtype WrapInstances g a = WrapInstances { unwrapInstances :: (g a) }

instance (Wrappable2 g a) => Eq (WrapInstances g a) where
  (==) = on eq1 unwrapInstances
  {-# INLINE (==) #-}

instance (Wrappable2 g a) => Hashable (WrapInstances g a) where
  hashWithSalt s = hashWithSalt1 s . unwrapInstances
  {-# INLINE hashWithSalt #-}

-- this is too general and breaks ghci:
-- λ> fromJust

-- <interactive>:40:1-8: error:
--     • No instance for (hashable-1.3.5.0:Data.Hashable.Class.Hashable1
--                          ((->) (Maybe ())))
--         arising from a use of ‘print’
--     • In a stmt of an interactive GHCi command: print it
-- instance (Wrappable2 g a) => Show (g a) where
--   show a = "showWrap: " <> builderToText (buildAtomInColumn1 a)
--   {-# INLINE show #-}

instance (Wrappable2 g a) => Ord (WrapInstances g a) where
  compare = on compare1 unwrapInstances
  {-# INLINE compare #-}

-- | the full blown version (which has an inverse: fromWrappedDynF (up to I None ~ Maybe a equivalence))
class Wrappable3 a where
  toWrappedDyn :: f a -> WrappedDyn f

instance Wrappable2 g a => Wrappable3 (g a) where 
  toWrappedDyn = WrappedDyn typeRep
  {-# INLINE toWrappedDyn #-}

class WrappableIorM f a where
  toWrappedDynIorM :: f a -> WrappedDyn f

instance {-# OVERLAPS #-} Wrappable a => WrappableIorM f (Maybe a) where 
  toWrappedDynIorM = WrappedDyn typeRep

instance {-# OVERLAPPING #-} (ICoerce f a, Wrappable a) => WrappableIorM f a where 
  toWrappedDynIorM = WrappedDyn typeRep . coerceI

instance Hashable1 I where
    liftHashWithSalt h salt (I x) = h salt x
  

-- * Wrapper to derive unlifted instances to be conveniently used in `zipDyn`
-- I think this is the same as WrapInstances, which could be replaced by this
newtype One g a = One (g a)

instance Wrappable2 g a => Hashable (One g a) where
  hashWithSalt salt = coerce (hashWithSalt1 @g @a salt)

instance Wrappable2 g a => Ord (One g a) where
  compare = coerce (compare1 @g @a)
  
instance Wrappable2 g a => Eq (One g a) where
  (==) = coerce (eq1 @g @a)

instance Wrappable2 g a => AtomShow (One g a) where
  buildAtomRaw = coerce (buildAtomRaw1 @g @a)

instance Wrappable2 g a => Show (One g a) where
  show = toS . toLazyText . buildAtomRaw

-- implements Nothing
newtype N x = N { unN :: x }

-- implements Ord only
newtype O x = O { unO :: x }

-- implements Show only
newtype S x = S { unS :: x }

errS :: forall x a . Typeable x => String -> S x -> a
errS msg _ = Prelude.error $ msg <> " not implemented for S (" <> show (R.typeRep @x) <> ")"

instance Typeable x => Eq (S x) where x == _ = errS "Eq" x
instance Typeable x => Ord (S x) where compare = errS "Ord"
instance Show x => Show (S x) where show = coerce (show @x)
instance Typeable x => Hashable (S x) where hashWithSalt _ = errS "Hashable"

errO :: forall x a . Typeable x => String -> O x -> a
errO msg _ = Prelude.error $ msg <> " not implemented for O (" <> show (R.typeRep @x) <> ")"

instance Eq x => Eq (O x) where (==) = coerce (\(x :: x) y -> (x == y))
instance (Eq x, Ord x) => Ord (O x) where compare = coerce (compare @x)
instance Typeable x => Show (O x) where show = errO "Show"
instance (Eq x, Typeable x) => Hashable (O x) where hashWithSalt _ = errO "Hashable"

errN :: forall x a . (HasCallStack, Typeable x) => String -> N x -> a
errN msg _ = Prelude.error $ msg <> " not implemented for N (" <> show (R.typeRep @x) <> ")"

instance Typeable x => Eq (N x) where x == _ = errN "Eq" x
instance Typeable x => Ord (N x) where compare = errN "Ord"
instance Typeable x => Show (N x) where show = errN "Show"
instance Typeable x => Hashable (N x) where hashWithSalt _ = errN "Hashable"

mapWrapInstances :: (UnWrapCoerce f g a, UnWrapCoerce h k b)
  => (f (WrapInstances g a) -> h (WrapInstances k b)) -> (f (g a) -> h (k b))
mapWrapInstances = coerce
{-# INLINE mapWrapInstances #-}

coerceUnWrapInstances :: UnWrapCoerce f g a => f (WrapInstances g a) -> f (g a)
coerceUnWrapInstances = coerce
{-# INLINE coerceUnWrapInstances #-}

coerceWrapInstances :: WrapCoerce f g a => f (g a) -> f (WrapInstances g a)
coerceWrapInstances = coerce
{-# INLINE coerceWrapInstances #-}


data WrappedDyn f = forall g a. (Wrappable2 g a) => WrappedDyn { wdType :: (TypeRep (g a))
                                                               , wdData :: !(f (g a)) }

-- data WrappedDyn f = forall g a. (Wrappable2 g a) => WrappedDyn { wdCoerce       :: g a -> H b
--                                                                , wdCoerceI      :: g a -> H b
--                                                                , wdData         :: !(f (g a)) }

-- makeLensesWithSuffix ''WrappedDyn

-- class CoerceH g a where
--   coerceH       :: g a -> H b
--   coerceIH      :: g a -> H b
--   coerceIH = coerceH

-- instance CoerceH I a where

makeLensesWithSuffix ''WrappedDyn

type TableCol' f = WrappedDyn f
type TableColH' f = H (TableCol' f)
type TableCol = TableCol' Vector
type TableColH = H TableCol
type TableCell = WrappedDyn I
type TableDict' f = Dict Symbols (Vector (TableCol' f))
type TableDict = TableDict' Vector
type TableRowDict = Dict Symbols (Vector TableCell)
type TableRow = Table' I
type TableRowH = H (Table' I)

data Table' f = UnsafeTableWithColumns { unsafeTableWithColumns :: !(TableDict' f)
                                       , unsafeTableRowNumbers  :: f Int
                                       }
  deriving (Generic)

type TableH' f = H (Table' f)
type Table = Table' Vector
type TableH = H Table
type GroupedCol f = TableCol' (f :.: Vector)
-- type GroupedTableRaw = Table' (Vector :.: Vector)
-- type GroupedTable = Dict Table GroupedTableRaw
type KeyedTable = Dict Table Table
type KeyedTableH = H KeyedTable
type SingleKeyTable a = Dict (Vector a) Table


data GroupedTable f     = GroupedTable
  { gtGroupSizes        :: f Int
  , gtToGroups          :: forall a . HasCallStack => Text -> Vector a -> H ((f :.: Vector) a)
  -- ^ takes the source vector and returns the groups
  , gtOriginalRowNumbers:: (f :.: Vector) Int
  , gtGroupRowNumbers   :: (f :.: Vector) Int
  , gtGroupNumber       :: (f :.: I) Int
  , gtTable             :: Table
                                        }


makeLensesWithSuffix ''Table'

-- NFData (f (g a)) would need to be added to the Wrappable constraints
-- instance NFData1 Table'

class FromTable a where
  fromTable :: HasCallStack => Table -> H a

class ToH Table a => ToTableAndBack a where
  type InH a :: Type
  unsafeBack :: HasCallStack => a -> TableH -> H' a

toTableAndBack :: (HasCallStack, ToTableAndBack b) => b -> (TableH, TableH -> H' b)
toTableAndBack = toH &&& unsafeBack

toTable :: (ToTable t, HasCallStack) => t -> TableH
toTable = toH

type H' a = H (InH a)

type ToTable a          = (HasCallStack, ToH Table a)
type ToTable2 a          = ToH Table a
type ToKeyedTable a     = (HasCallStack, ToH KeyedTable a)

instance ToH Table Table                where toH = pure
instance ToH Table TableH               where toH = id
-- instance ToH Table (TypedTableOld r)       where toH = pure . fromTypedTableOld
-- instance ToH Table (H (TypedTableOld r))   where toH = fmap fromTypedTableOld

instance ToH KeyedTable KeyedTable                      where toH = pure
instance ToH KeyedTable KeyedTableH                     where toH = id
-- instance ToH KeyedTable (KeyedTypedTableOld k v)           where toH = pure . fromKeyedTypedTableOld
-- instance ToH KeyedTable (H (KeyedTypedTableOld k v))       where toH = fmap fromKeyedTypedTableOld


withWrapped :: (forall a g. (Wrappable2 g a) => f (g a) -> b) -> WrappedDyn f -> b
withWrapped f (WrappedDyn _ wd) = f wd
{-# INLINE withWrapped #-}

withWrapped' :: (forall a g. (Wrappable2 g a) => TypeRep (g a) -> f (g a) -> b) -> WrappedDyn f -> b
withWrapped' f (WrappedDyn tr wd) = f tr wd
{-# INLINE withWrapped' #-}

mapTableCol :: WrapCoerce Vector g a
  => (forall a . WrapInstancesClass a => Vector a -> Vector a) -> TableCol -> TableCol
mapTableCol f (WrappedDyn t wd) = WrappedDyn t $ mapWrapInstances f wd
{-# INLINE mapTableCol #-}

mapWrappedM :: Functor m => (forall g a. (Wrappable2 g a)
  => f (g a) -> m (h (g a))) -> WrappedDyn f -> m (WrappedDyn h)
mapWrappedM f (WrappedDyn t wd) = WrappedDyn t <$> f wd
{-# INLINE mapWrappedM #-}

mapWrapped :: (forall g a. (Wrappable2 g a) => f (g a) -> h (g a)) -> WrappedDyn f -> WrappedDyn h
mapWrapped f (WrappedDyn t wd) = WrappedDyn t $ f wd
{-# INLINE mapWrapped #-}

-- unsafeCoerceWrappedDyn :: Coercible f g => WrappedDyn f -> WrappedDyn g
-- unsafeCoerceWrappedDyn = unsafeCoerce

-- instance NFData (WrappedDyn f) where
  -- rnf = withWrapped' $ \t d -> rnf (SomeTypeRep t) `seq` rnf d


tableCell :: TableCol' (I :.: I) -> TableCol' I
tableCell = unsafeCoerce

aggregatedTableCol :: TableCol' (Vector :.: I) -> TableCol' Vector
aggregatedTableCol = unsafeCoerce

singleGroupTableCol :: TableCol' (I :.: Vector) -> TableCol' Vector
singleGroupTableCol = unsafeCoerce

toSingleGroupTableCol :: TableCol' Vector -> TableCol' (I :.: Vector)
toSingleGroupTableCol = unsafeCoerce


-- mapWrappedD :: (Functor f, Wrappable b, Wrappable1 k) => (f b -> h (k b)) -> WrappedDyn f -> WrappedDyn h
-- mapWrappedD f = toWrappedDyn . f . fromWrappedDyn

-- instance Show (WrappedDyn f) where show = withWrapped show

flipTable :: Table' f -> TableDict' f
flipTable = unsafeTableWithColumns
{-# INLINE flipTable #-}

cols :: Table' f -> Symbols
cols = key . flipTable

vals :: Table' f -> Vector (TableCol' f)
vals = value . flipTable

data TableKey' f        = UnsafeTableKey { tTable :: Table' f
                                         , tIdx   :: {-# UNPACK #-} !IterIndex
                                         }

type TableKey = TableKey' Vector

instance Show TableKey where
  show (UnsafeTableKey _ i) = "TableKey " <> show i

-- * construction
  
table :: (ToVector f, HasCallStack) => f (Symbol, TableCol) -> TableH
table = chain table2 . uncurry dict . V.unzip . toVector
{-# INLINABLE table #-}

tableRowAnonymous :: HasCallStack => [(Maybe Symbol, TableCell)] -> TableRowH
tableRowAnonymous = fmap tableRow . toDictAnonymous
{-# INLINABLE tableRowAnonymous #-}

groupedTableAnonymous :: HasCallStack
  => Grouping -> GroupedTable Vector -> [(Maybe Symbol, GroupedCol Vector)] -> TableH
groupedTableAnonymous ging g = tableFromGroupedCols2 ging g <=< toDictAnonymous
  -- . (\x -> trace @String (show $ fmap2 (withWrapped asd) x) x)
{-# INLINABLE groupedTableAnonymous #-}

-- asd :: (Functor f, Show (f (One g a)), Wrappable2 g a) => f (g a) -> String
-- asd = show . fmap One

tableAnonymous :: HasCallStack => [(Maybe Symbol, TableCol)] -> TableH
tableAnonymous = chain table2 . toDictAnonymous
{-# INLINABLE tableAnonymous #-}

toDictAnonymous :: [(Maybe Symbol, a)] -> VectorDictH Symbol a
toDictAnonymous = (\(k,v) -> dict (V.fromList $ makeUniqueNames k) $ V.fromList v) . unzip
{-# INLINABLE toDictAnonymous #-}

-- | inspired by q:
-- 1. unnamed columns get called x
-- 2. duplicate columns get suffixed by an number
-- 3. this function can produce duplicates when the input already contains number-suffixed
--    columns, e.g. [a,a1] -> [a1,a1]
makeUniqueNames :: [Maybe Symbol] -> [Symbol]
makeUniqueNames cols = runST $ do
  ht <- HT.newSized $ length cols
  let appendNumber name = HT.mutate ht name $ \case
        Nothing -> (Just (1 :: Int), name)
        Just c  -> (Just $ succ c, name <> showt c)
  forM cols $ appendNumber . fromMaybe "x"

-- | this is a total function because only tables with columns can be constructed
firstCol :: Table -> TableCol
firstCol = (V.! 0) . vals

tableFromGroupedCols2 :: HasCallStack
  => Grouping -> GroupedTable Vector -> TableDict' (Vector :.: Vector) -> TableH
tableFromGroupedCols2 ging g d = case equalLength $ value ls of
  Just (_, True)-> throwH $ TableDifferentColumnLenghts $ show ls
  Nothing       -> throwH $ TableWithNoColumn "not allowed"
  _  -> pure $ unsafeTableWithRowIndices $ mapDictValues (mapWrapped $ redistributeGroups ging)  d
  where ls = (\d -> dictNoLengthCheck (V.cons "gtGroupSizes" $ key d) $ V.cons (gtGroupSizes g) $ value d)
             $ mapDictValues (withWrapped $ fmap V.length . unComp) d

redistributeGroups :: Grouping -> (Vector :.: Vector) a -> Vector a
redistributeGroups = (\idxs2 -> unsafeBackpermute idxs2 . V.concatMap id . unComp) . groupInversionIndices

table2 :: HasCallStack => TableDict -> TableH
table2 d = case equalLength $ value ls of
  Just (_, True)    -> throwH $ TableDifferentColumnLenghts $ show ls
  Nothing               -> throwH $ TableWithNoColumn "not allowed"
  _                     -> pure $ unsafeTableWithRowIndices d
  where ls = mapDictValues (withWrapped $ I . V.length) d

tableNoLengthCheck :: HasCallStack => TableDict -> TableH
tableNoLengthCheck x | count x > 0      = pure $ unsafeTableWithRowIndices x
                     | True             = throwH $ TableWithNoColumn ""

tableRow :: TableRowDict -> TableRow
tableRow = flip UnsafeTableWithColumns $ I 0

unsafeTableWithRowIndices :: TableDict -> Table
unsafeTableWithRowIndices d = r
  where r = UnsafeTableWithColumns d $ V.enumFromN 0 $ count r
{-# INLINABLE unsafeTableWithRowIndices #-}

-- class (Foldable f, Functor f, Eq1 f) => GroupConcatable' f where
--   concatGroups :: VectorDict Symbol (TableCol' f) -> TableDict

-- type GroupConcatable f = GroupConcatable' (f :.: Vector)

-- instance GroupConcatable' (I :.: Vector) where
--   concatGroups = unsafeCoerce :: VectorDict Symbol (TableCol' (I :.: Vector)) -> VectorDict Symbol TableCol

-- instance GroupConcatable' (Vector :.: Vector) where
--   concatGroups = mapDictValues $ mapWrapped $ V.concatMap id . unComp

-- instance GroupConcatable' Vector where
--   concatGroups = id
  

equalLength :: Eq1 f => Vector (f Int) -> Maybe ((f Int), Bool)
equalLength = fmap (\(c, cs) -> seq c $ (c, V.any (not . eq1 c) cs)) . V.uncons

tc' :: forall a f . (HasCallStack, ICoerce f a, ToVector f, Wrappable a) => Symbol -> f a -> Table
tc' name = tcF' name . coerceI
{-# INLINABLE tc' #-}

tcF' :: forall a f h. (HasCallStack, ToVector h, Wrappable a, Wrappable1 f) => Symbol -> h (f a) -> Table
tcF' name v = unsafeTableWithRowIndices $ dictNoLengthCheck (pure name) $ pure $ toWrappedDyn $ toVector v
{-# INLINABLE tcF' #-}

tc :: forall a f . (HasCallStack, ICoerce f a, ToVector f, Wrappable a) => Symbol -> f a -> TableH
tc name = tcF name . coerceI
{-# INLINABLE tc #-}

tcF :: forall a f h. (HasCallStack, ToVector h, Wrappable a, Wrappable1 f) => Symbol -> h (f a) -> TableH
tcF = fmap2 pure tcF'
{-# INLINABLE tcF #-}

(<#) :: (HasCallStack, ICoerce f a, ToVector f, Wrappable a) => Symbol -> f a -> TableH
(<#) = tc
{-# INLINE (<#) #-}

(<:) :: forall a f h. (HasCallStack, ToVector h, Wrappable a, Wrappable1 f) => Symbol -> h (f a) -> TableH
(<:) = fmap2 pure tcF'
{-# INLINABLE (<:) #-}
 
infix 3 <#
infix 3 <:

-- * instances 

instance Eq Table where
  (==) = on (==) flipTable

instance Eq TableCol where
  (WrappedDyn t1 v1) == (WrappedDyn t2 v2)
    | Just HRefl <- t1 `eqTypeRep` t2   = on (==) coerceWrapInstances v1 v2
    |  True                             = False

-- | careful: this instance assumes that key t1 == key t2, and typeOf for the column vectors
instance Eq TableKey where
  UnsafeTableKey t1 (IterIndex i1) == UnsafeTableKey t2 (IterIndex i2)
    = V.and $ on (V.zipWith compareCell) vals t1 t2  
    where compareCell (WrappedDyn _ v1) (WrappedDyn _ v2) =
            V.unsafeIndex v1 i1 `eq1` unsafeCoerce (V.unsafeIndex v2 i2)
            -- | Just HRefl <- t1 `eqTypeRep` t2 = v1 V.! i1 == v2 V.! i2
            -- | True                            = False 
          compareCell :: TableCol -> TableCol -> Bool

instance Hashable TableKey where
  hashWithSalt salt (UnsafeTableKey t (IterIndex i)) = hashWithSaltFold salt
    $ \hash -> (withWrapped $ \cv -> hash $ V.unsafeIndex cv i) <$> vals t


hashWithSaltFold :: Foldable f
  => Int -> (forall b . (forall a g . (Hashable1 g, Hashable a) => (g a) -> b) -> f b) -> Int
hashWithSaltFold salt g = foldl' (&) salt $ g $ flip hashWithSalt1
{-# INLINE hashWithSaltFold #-}

-- hashWithSaltFold :: Foldable f => Int -> (forall b . (forall x . Hashable x => x -> b) -> f b) -> Int
-- hashWithSaltFold salt g = foldl' (\b a -> a b) salt $ g $ flip hashWithSalt

instance Iterable Table where
  count = withWrapped V.length . firstCol

  null = withWrapped V.null . firstCol

  take n = mapTableWrappedNoLengthCheck $ H.take n

  drop n = mapTableWrappedNoLengthCheck $ H.drop n

  unsafeBackpermute ks = mapTableWrappedNoLengthCheck $ flip G.unsafeBackpermute $ coerce ks

  reverse = mapTableWrappedNoLengthCheck V.reverse

                                

-- * zip fold map

zipTable :: (Symbol -> TableCol' f -> a) -> Table' f -> Vector a
zipTable f t = V.zipWith f (cols t) $ vals t
{-# INLINABLE zipTable #-}

zipTableDict3M :: Monad m
  => (Symbol -> TableCol' f -> c -> m a) -> TableDict' f -> Vector c -> m (Vector a)
zipTableDict3M f t = sequence . V.zipWith3 f (key t) (value t)
{-# INLINABLE zipTableDict3M #-}

zipTable3M :: Monad m
  => (Symbol -> TableCol' f -> c -> m a) -> Table' f -> Vector c -> m (Vector a)
zipTable3M f t = sequence . V.zipWith3 f (cols t) (vals t)
{-# INLINABLE zipTable3M #-}

zipTable3 :: (Symbol -> TableCol' f -> c -> a) -> Table' f -> Vector c -> Vector a
zipTable3 f t = V.zipWith3 f (cols t) $ vals t
{-# INLINABLE zipTable3 #-}

tableRowToTable :: TableRow -> Table
tableRowToTable r = UnsafeTableWithColumns dict $ V.singleton 0
  where dict = mapDictValues (mapWrapped $ V.singleton . unI) $ flipTable r
  
mapTableWithNameNoLengthCheck :: (Symbol -> TableCol -> TableCol) -> Table -> Table
mapTableWithNameNoLengthCheck f = unsafeTableWithRowIndices . mapDictWithKey f . flipTable

mapMTableWithNameNoLengthCheck :: Monad m => (Symbol -> TableCol -> m TableCol) -> Table -> m Table
mapMTableWithNameNoLengthCheck f = fmap unsafeTableWithRowIndices . mapDictWithKeyM f . flipTable

mapTableNoLengthCheck :: (TableCol -> TableCol) -> Table -> Table
mapTableNoLengthCheck f = unsafeTableWithRowIndices . mapDictValues f . flipTable

mapTableWrappedNoLengthCheck :: (forall g a. (Wrappable2 g a)
  => Vector (g a) -> Vector (g a)) -> Table -> Table
mapTableWrappedNoLengthCheck f = mapTableNoLengthCheck $ mapWrapped f

-- * Pairs of WrappedDyns (for zipping, useful for sorting)

zipDyn :: forall f . Zippable f => TableCol' f -> TableCol' f -> TableCol' f
zipDyn (WrappedDyn _ w1) (WrappedDyn _ w2) = WrappedDyn typeRep $ g w1 w2
  where g :: forall g a g2 b . (Wrappable2 g a, Wrappable2 g2 b) => f (g a)
          -> f (g2 b) -> f (I (One g a,One g2 b))
        g = zipWith_ (\a b -> coerce (a,b))

zip3Dyn :: forall f . Zippable f => TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f
zip3Dyn (WrappedDyn _ w1) (WrappedDyn _ w2) (WrappedDyn _ w3) = WrappedDyn typeRep $ g w1 w2 w3
  where g :: forall g a g2 b g3 c . (Wrappable2 g a, Wrappable2 g2 b, Wrappable2 g3 c) => f (g a)
          -> f (g2 b) -> f (g3 c) -> f (I (One g a,One g2 b, One g3 c))
        g = zipWith3_ (\a b c -> coerce (a,b,c))

zip4Dyn :: forall f . Zippable f
  => TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f
zip4Dyn (WrappedDyn _ w1) (WrappedDyn _ w2) (WrappedDyn _ w3) (WrappedDyn _ w4)
  = WrappedDyn typeRep $ g w1 w2 w3 w4
  where g :: forall g a g2 b g3 c g4 d.
          (Wrappable2 g a, Wrappable2 g2 b, Wrappable2 g3 c , Wrappable2 g4 d)
          => f (g a) -> f (g2 b) -> f (g3 c) -> f (g4 d)
          -> f (I (One g a,One g2 b, One g3 c, One g4 d))
        g = zipWith4_ (\a b c d -> coerce (a,b,c,d))


zip5Dyn :: forall f . Zippable f
  => TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f
zip5Dyn (WrappedDyn _ w1) (WrappedDyn _ w2) (WrappedDyn _ w3) (WrappedDyn _ w4) (WrappedDyn _ w5)
  = WrappedDyn typeRep $ g w1 w2 w3 w4 w5
  where g :: forall g a g2 b g3 c g4 d g5 e.
          (Wrappable2 g a, Wrappable2 g2 b, Wrappable2 g3 c, Wrappable2 g4 d, Wrappable2 g5 e)
          => f (g a) -> f (g2 b) -> f (g3 c) -> f (g4 d) -> f (g5 e)
          -> f (I (One g a,One g2 b, One g3 c, One g4 d, One g5 e))
        g = zipWith5_ (\a b c d e -> coerce (a,b,c,d,e))

zip6Dyn :: forall f . Zippable f
  => TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f -> TableCol' f
zip6Dyn (WrappedDyn _ w1) (WrappedDyn _ w2) (WrappedDyn _ w3) (WrappedDyn _ w4) (WrappedDyn _ w5)
  (WrappedDyn _ w6) = WrappedDyn typeRep $ g w1 w2 w3 w4 w5 w6
  where g :: forall g a g2 b g3 c g4 d g5 e g6 x.
          (Wrappable2 g a, Wrappable2 g2 b, Wrappable2 g3 c, Wrappable2 g4 d, Wrappable2 g5 e
          , Wrappable2 g6 x) => f (g a) -> f (g2 b) -> f (g3 c) -> f (g4 d)
          -> f (g5 e) -> f (g6 x)
          -> f (I (One g a,One g2 b, One g3 c, One g4 d, One g5 e, One g6 x))
        g = zipWith6_ (\a b c d e f -> coerce (a,b,c,d,e,f))

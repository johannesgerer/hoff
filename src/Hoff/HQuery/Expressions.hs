{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Hoff.HQuery.Expressions where

import           Hoff.Utils
import           Control.Lens hiding (Empty, (<.))
import           Control.Monad.Reader
import           Data.Coerce
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import           Data.String
import           Data.Vector (Vector)
import qualified Data.Maybe as P
import qualified Data.Vector as V
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Table as H
import           Hoff.TypedTable
import           Hoff.Utils
import           Hoff.Vector
import           Type.Reflection as R
import           Yahp hiding (reader, ask, (:.:), group, delete, take, filter, (<|>), unlines)

newtype NamedTableReader f a = NamedTableReader
  { fromNamedTableReader :: (Maybe Symbol, ReaderT (GroupedTable f) H a) }
  deriving (Functor)

-- | f = Aggregation type: `I` (full table) or `Vector` (groups)
--   g = Result: `f` (aggregated) or `f :.: Vector` (homogenous)
newtype HQuery f  g a   = HQuery        { unHQuery      :: NamedTableReader f (g a) }
  deriving Functor

newtype HQueryDyn f g   = HQueryDyn     { unHQueryDyn   :: NamedTableReader f (TableCol' g) }

data GroupedTable f     = GroupedTable  { gtGroupSizes :: f Int
                                        , gtToGroups   :: forall a . Vector a -> f (Vector a)
                                        -- ^ takes the source vector and returns the groups
                                        , gtTable      :: Table
                                        }

makeLensesWithSuffix ''NamedTableReader
makeLensesWithSuffix ''HQueryDyn
makeLensesWithSuffix ''HQuery


-- ** Regular expressions

type Exp f      = HQuery f (f :.: Vector)

type ExpDyn f   = HQueryDyn f (f :.: Vector)
type ExpDyns f  = [ExpDyn f]

-- ** Aggregation expressions

type Agg f a    = HQuery f f a

type AggDyn f   = HQueryDyn f f
type AggDyns f  = [AggDyn f]


instance Applicative (NamedTableReader f) where
  pure = NamedTableReader . (Nothing,) . pure
  (NamedTableReader (nf,f)) <*> (NamedTableReader (nx,x)) = NamedTableReader (nx `mplus` nf, f <*> x)
  {-# INLINABLE pure #-}

-- * Sources for table columns / vectors

-- -- *** Table columns

-- -- | get static table column
-- cl :: (HasCallStack, Typeable a) => Symbol -> Exp a
-- cl = tableCol
-- {-# INLINABLE cl #-}

-- -- | convert to maybe column
-- mc :: (HasCallStack, Typeable a) => Symbol -> Exp (Maybe a)
-- mc n = liftC' (chain (fromWrappedDyn $ Just n) . toMaybe' n) $ tableColD n
-- {-# INLINABLE mc #-}

-- -- | see `tableColAgg`
-- aa :: (HasCallStack, Typeable a) => Symbol -> Agg (Vector a)
-- aa = tableColAgg
-- {-# INLINABLE aa #-}

-- -- | get dynamic table column
-- ad :: HasCallStack => Symbol -> ExpDyn
-- ad = tableColD

-- -- | explicitly renamed dynamic table column
-- ed :: HasCallStack => Symbol -> Symbol -> ExpDyn
-- ed n = setName n . ad

-- | row number (zero-based)
-- rn :: Int
-- rn = HQueryDyn $ NamedTableReader (Nothing, ReaderT $ pure . toWrappedDynI . unsafeTableRowNumbers)

-- | row number in group (zero-based)
-- grn = 

-- | unwrap I, leave other constructors untouched
tableCol :: (ColumnData a f, HasCallStack) => Symbol -> Exp f a
tableCol n = fromDyn (Just n) $ tableColD n
{-# INLINABLE tableCol #-}

-- | unwrap I, leave other constructors untouched
fromDyn :: (ColumnData a f, HasCallStack) => Maybe Symbol -> ExpDyn f -> Exp f a
fromDyn = liftC' . fromWrappedDyn
{-# INLINABLE fromDyn #-}

-- | access columns -- `Int`
tableColD :: HasCallStack => Symbol -> ExpDyn f
tableColD name = HQueryDyn $ NamedTableReader $ (Just name, ReaderT g)
  where g GroupedTable{..} = mapHException (appendMsg $ toS $ "\n\n" <> showMetaWithDimensions gtTable)
          $ mapWrapped (Comp . gtToGroups) <$> flipTable gtTable ! name
{-# INLINE tableColD #-}

-- -- test :: HasCallStack => Exp Int
-- -- test = noName $ ReaderT $ \t -> fromWrappedDyn =<< flipTable (fst t) ! "asd"

-- -- -- | this cannot be done currectly in the current design because (I
-- -- -- None) columns need to remain castable to Maybe a for any a.
-- -- -- To achieve this, we would need to add Vector to the allowed constructors inside a Wrappable
-- -- tableColAggD :: (HasCallStack) => Symbol -> AggDyn
-- -- tableColAggD name = NamedTableReader (Just name, ReaderT $ \(t, GroupedTable _ grouper) ->
-- --                                          asd grouper <$> flipTable t ! name)
-- -- {-# INLINABLE tableColAggD #-}

-- -- asd :: (forall a . Vector a -> Vector (Vector a)) -> TableCol -> TableCol
-- -- asd grouper = \case
-- --   c@(WrappedDyn tr@(App con _) v)
-- --     | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> pure mempty
-- --     | Just HRefl <- con   `eqTypeRep` typeRep @I        -> fromWrappedDyn c
-- --     | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> fromWrappedDyn $ toWrappedDynI $ V.catMaybes v
-- --   w             -> errorMaybeOrI "n/a" w


-- -- | aggregates the values of the given static query for each group into a vector
-- expVectorAgg :: (HasCallStack, Typeable a) => Exp f a -> Agg f (Vector a)
-- expVectorAgg = mapA id
-- {-# INLINE expVectorAgg #-}

-- -- | aggregates the column values for each group into a vector
-- tableColAgg :: (HasCallStack, Typeable a) => Symbol -> Agg f (Vector a)
-- tableColAgg = expVectorAgg . tableCol
-- {-# INLINABLE tableColAgg #-}

-- * Instances 

instance {-# OVERLAPS #-} IsString [ExpDyn f] where
  fromString = pure . tableColD . toS
  
instance IsString (ExpDyn f) where
  fromString = tableColD . toS

instance (ColumnData a f) => IsString (Exp f a) where
  fromString = tableCol . toS
  {-# INLINABLE fromString #-}
  
instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m (ExpDyns f) where
  fromLabel = [fromLabel @m]
  {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m (ExpDyn f) where
  fromLabel = tableColD $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (ColumnData a f, KnownSymbol m) => IsLabel m (Exp f a) where
  fromLabel = tableCol $ fromLabel @m
  {-# INLINABLE fromLabel #-}
  
-- -- instance (KnownSymbol m) => IsLabel m AggDyn where
-- --   fromLabel :: HasCallStack => AggDyn
-- --   fromLabel = tableColAggD $ fromLabel @m
-- --   {-# INLINABLE fromLabel #-}

-- instance {-# OVERLAPS #-} (Typeable a, KnownSymbol m) => IsLabel m (Agg f (Vector a)) where
--   fromLabel = tableColAgg $ fromLabel @m
--   {-# INLINABLE fromLabel #-}

-- instance Typeable a => IsString (Agg f (Vector a)) where
--   fromString = tableColAgg . toS
--   {-# INLINABLE fromString #-}
  


-- * Sinks

-- -- ** Exp 

-- -- *** automatically named

-- | automatically named / `I a` value
ai :: forall a g f . (ICoerce g a, Wrappable a) => HQuery f g a -> HQueryDyn f g
ai = HQueryDyn . fmap toWrappedDynI . unHQuery
{-# INLINE ai #-}

-- | automatically named / `Maybe a` value
am :: (Wrappable a) => HQuery f g (Maybe a) -> HQueryDyn f g
am = af
{-# INLINE am #-}

-- | automatically named / `f a` value
af :: forall g a f h . (Wrappable a, Wrappable1 g) => HQuery f h (g a) -> HQueryDyn f h
af = HQueryDyn . fmap toWrappedDyn . unHQuery
{-# INLINE af #-}

-- -- *** explicitly named

-- -- | explicitly named / `I a` value
-- ei :: (Wrappable a) => Symbol -> HQuery f a -> HQueryDyn f
-- ei name = setName name . ai
-- {-# INLINE ei #-}

-- infix 3 </

-- -- | explicitly named / `I a` value
-- (</) :: (HasCallStack, Wrappable a) => Symbol -> HQuery f a -> HQueryDyn f
-- (</) = ei
-- {-# INLINE (</) #-}

-- infix 3 <?
-- -- | explicitly named / `Maybe a` value
-- (<?) :: (Wrappable a) => Symbol -> HQuery f (Maybe a) -> HQueryDyn f
-- (<?) = em
-- {-# INLINE (<?) #-}


-- -- | explicitly named / `f a` value
-- ef :: forall g a f . (Wrappable1 g, Wrappable a) => Symbol -> HQuery f (g a) -> HQueryDyn f
-- ef name = setName name . af
-- {-# INLINE ef #-}

-- em :: (Wrappable a) => Symbol -> HQuery f (Maybe a) -> HQueryDyn f
-- em = ef
-- {-# INLINE em #-}

                
-- sn :: Symbol -> HQueryDyn f -> HQueryDyn f
-- sn = setName
-- {-# INLINE sn #-}

-- -- ** Exp sources

-- -- *** Constants

-- -- | named constant with `f a` value, e.g. `Maybe a` 
-- cm :: forall a g f . (HQueryContext f, Wrappable1 g, Wrappable a) => Symbol -> g a -> HQueryDyn f
-- cm n = ef n . co
-- {-# INLINE cm #-}

-- -- | named constant with `I a` value
-- ci :: forall a f . (HQueryContext f, Wrappable a) => Symbol -> a -> HQueryDyn f
-- ci n = ei n . co
-- {-# INLINE ci #-}

-- class HQueryContext a where
--   contextCount :: a -> Int

-- instance HQueryContext Table where
--   contextCount = count
--   {-# INLINE contextCount #-}

-- instance HQueryContext GroupedTable where
--   contextCount = count . gtGroupSizes
--   {-# INLINE contextCount #-}

-- -- | constant static value
-- co :: forall a f . HQueryContext f => a -> HQuery f a
-- co x = noName $ reader $ flip V.replicate x . contextCount
-- {-# INLINE co #-}

-- -- | use aggregation result as constant value
-- ca :: HasCallStack => Agg a -> Exp a
-- ca = mapNTR $ \agr -> ReaderT $ \t -> replicateSameLength t . V.head <$> runReaderT agr (fullGroup t)
-- {-# INLINE ca #-}


-- -- | for each group, distribute the aggregated value back to the whole group
-- fby :: HasCallStack => Agg a -> ExpDyns -> Exp a
-- fby agg bys = flip mapNTR agg $ \agg' -> ReaderT $ \t -> do
--   idxs <- snd . getGroupsAndGrouping <$> select bys t
--   broadcastGroupValue idxs <$> runReaderT agg' (fromGrouping idxs t)
-- {-# INLINABLE fby #-}

-- -- * Window functions

-- -- | for each group, generate a new vector with one entry for each row in the group
-- aggVectorDistributer :: HasCallStack => Agg (Vector a) -> ExpDyns -> Exp a
-- aggVectorDistributer agg bys = flip mapNTR agg $ \agg' -> ReaderT $ \t -> do
--   idxs <- snd . getGroupsAndGrouping <$> select bys t
--   unsafeBackpermute (V.concatMap id idxs) . V.concatMap id <$> runReaderT agg' (fromGrouping idxs t)
-- {-# INLINABLE aggVectorDistributer #-}

-- -- | for each group, generate the resulting vector and distribute it back to the original row
-- -- for functions that do not depend on the order of the input, we have `eby = const`
-- --
-- -- the function must not change the length of the vector
-- eby :: (HasCallStack, Typeable a) => (Vector a -> Vector b) -> ExpDyns -> Exp a -> Exp b
-- eby f bys exp' = flip mapNTR exp' $ \exp -> ReaderT $ \t -> do
--   idxs <- snd . getGroupsAndGrouping <$> select bys t
--   expVs <- applyGrouping idxs <$> runReaderT exp t
--   let resVs = f <$> expVs
--   if fmap length expVs == fmap length resVs then
--     pure $ unsafeBackpermute (V.concatMap id idxs) $ V.concatMap id resVs
--     else throwH $ CountMismatch $ unlines $ "Function returned vector of different length:\n" :
--          toList (V.zipWith (\a b -> show (length a, length b)) expVs (V.take 10 resVs))
-- {-# INLINE eby #-}


-- nextD :: HasCallStack => HQueryDyn t -> HQueryDyn t
-- nextD = HQueryDyn . liftCWithName (modifyMaybeCol $ Just $ TableColModifierWithDefault nextV)
-- {-# INLINABLE nextD #-}

prevD :: (ColumnDataD f, HasCallStack) => ExpDyn f -> ExpDyn f
prevD = HQueryDyn . liftCWithName (modifyMaybeCol $ Just $ TableColModifierWithDefault $ fmapComp . prevV)
{-# INLINABLE prevD #-}

nextD :: (ColumnDataD f, HasCallStack) => ExpDyn f -> ExpDyn f
nextD = HQueryDyn . liftCWithName (modifyMaybeCol $ Just $ TableColModifierWithDefault $ fmapComp . nextV)
{-# INLINABLE nextD #-}

next :: forall a f . Functor f => Exp f a -> Exp f (Maybe a)
next = liftE $ nextV Nothing . fmap Just
{-# INLINABLE next #-}

prev :: forall a f . Functor f => Exp f a -> Exp f (Maybe a)
prev =  liftE $ prevV Nothing . fmap Just
{-# INLINABLE prev #-}

prevM :: forall a f . Functor f => Exp f (Maybe a) -> Exp f (Maybe a)
prevM = liftE $ prevV Nothing
{-# INLINABLE prevM #-}

nextM :: forall a f . Functor f => Exp f (Maybe a) -> Exp f (Maybe a)
nextM = liftE $ nextV Nothing
{-# INLINABLE nextM #-}

prevV :: a -> Vector a -> Vector a
prevV d = V.cons d . V.init
{-# INLINABLE prevV #-}

nextV :: b -> Vector b -> Vector b
nextV d = flip V.snoc d . V.tail
{-# INLINABLE nextV #-}
  
-- -- *** explicit vectors
-- --
-- -- most functions (like `select`) that consume these will check if the vector have the correct length

-- vec :: Vector a -> HQuery f a
-- vec v = noName $ reader $ const v


-- -- | named vector with implicit `I a` type
-- vi :: Wrappable a => Symbol -> Vector a -> HQueryDyn f
-- vi n = ei n . vec

-- -- | named vector with explicit `f a` type
-- vf :: forall a g f . (Wrappable1 g, Wrappable a) => Symbol -> Vector (g a) -> HQueryDyn f
-- vf n = ef n . vec



-- * Helpers

-- noName :: ReaderT f H (Vector p) -> HQuery f p
-- noName = HQuery . NamedTableReader . (Nothing,)
-- {-# INLINABLE noName #-}

-- withName :: Symbol -> ReaderT f H (Vector p) -> HQuery f p
-- withName name = HQuery . NamedTableReader . (Just name,)
-- {-# INLINABLE withName #-}

-- nameLens :: (Maybe Symbol -> Identity (Maybe Symbol)) -> HQueryDyn f -> Identity (HQueryDyn f)
-- nameLens = unHQueryDyn_ . fromNamedTableReader_ . _1
-- {-# INLINABLE nameLens #-}

-- setName :: Symbol -> HQueryDyn f -> HQueryDyn f
-- setName name = nameLens .~ Just name
-- {-# INLINABLE setName #-}

-- mapNTR :: (ReaderT f H (Vector a1) -> ReaderT g H (Vector a2)) -> HQuery f a1 -> HQuery g a2
-- mapNTR f = HQuery . NamedTableReader . second f . fromNamedTableReader . unHQuery
-- {-# INLINABLE mapNTR #-}

mapNTR2 :: (a -> H b) -> NamedTableReader f a -> NamedTableReader f b
mapNTR2 f = fromNamedTableReader_ . _2 %~ mapReaderT (chain f)
{-# INLINABLE mapNTR2 #-}

withHQuery :: (NamedTableReader f1 (g1 a1) -> NamedTableReader f2 (g2 a2)) -> HQuery f1 g1 a1 -> HQuery f2 g2 a2
withHQuery f = HQuery . f . unHQuery
{-# INLINE withHQuery #-}

withHQueryDyn :: (NamedTableReader f1 (TableCol' g1) -> NamedTableReader f2 (TableCol' g2))
  -> HQueryDyn f1 g1 -> HQueryDyn f2 g2
withHQueryDyn f = HQueryDyn . f . unHQueryDyn
{-# INLINE withHQueryDyn #-}

withHqDynStatic :: (NamedTableReader f1 (TableCol' g1) -> NamedTableReader f2 (g2 a))
  -> HQueryDyn f1 g1 -> HQuery f2 g2 a
withHqDynStatic f = HQuery . f . unHQueryDyn
{-# INLINE withHqDynStatic #-}

withComp :: (f (g a) -> f (h b)) -> (f :.: g) a -> (f :.: h) b
withComp = coerce
{-# INLINE withComp #-}

fmapComp :: Functor f => (g a -> h b) -> (f :.: g) a -> (f :.: h) b
fmapComp = withComp . fmap



liftC :: (TableCol' g -> TableColH' g) -> HQueryDyn f g -> HQueryDyn f g
liftC =  withHQueryDyn . mapNTR2
{-# INLINABLE liftC #-}

liftC' :: (TableCol' g -> H (g p)) -> HQueryDyn f g -> HQuery f g p
liftC' =  withHqDynStatic . mapNTR2
{-# INLINABLE liftC' #-}

liftCWithName :: (Maybe Symbol -> TableCol' g -> H a) -> HQueryDyn f g -> NamedTableReader f a
liftCWithName f (HQueryDyn (NamedTableReader (n, r))) = NamedTableReader (n, mapReaderT (chain $ f n) r)
{-# INLINE liftCWithName #-}

-- liftE6 :: forall a b c d e f x t .
--   (Vector a -> Vector b -> Vector c -> Vector d -> Vector e -> Vector f -> Vector x)
--   -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e -> HQuery t f -> HQuery t x
-- liftE6 f = coerce $ liftA6 @(NamedTableReader t) f
-- {-# INLINE liftE6 #-}

-- liftE5 :: forall a b c d e x t . (Vector a -> Vector b -> Vector c -> Vector d -> Vector e -> Vector x)
--   -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e -> HQuery t x
-- liftE5 f = coerce $ liftA5 @(NamedTableReader t) f
-- {-# INLINE liftE5 #-}

-- liftE4 :: forall a b c d x t . (Vector a -> Vector b -> Vector c -> Vector d -> Vector x)
--   -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t x
-- liftE4 f = coerce $ liftA4 @(NamedTableReader t) f
-- {-# INLINE liftE4 #-}

-- liftE3 :: forall a b c x t .
--   (Vector a -> Vector b -> Vector c -> Vector x) -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t x
-- liftE3 f = coerce $ liftA3 @(NamedTableReader t) f
-- {-# INLINE liftE3 #-}

-- liftE2 :: forall a b c t . (Vector a -> Vector b -> Vector c) -> HQuery t a -> HQuery t b -> HQuery t c
-- liftE2 f = coerce $ liftA2 @(NamedTableReader t) f
-- {-# INLINE liftE2 #-}

-- liftEH :: (Vector a -> VectorH b) -> HQuery t a -> HQuery t b
-- liftEH f = unHQuery_ . fromNamedTableReader_ . _2 %~ mapReaderT (chain f)
-- {-# INLINE liftEH #-}

-- liftE1flip :: (a2 -> a1 -> c) -> a1 -> HQuery t a2 -> HQuery t c
-- liftE1flip = liftE1 . flip
-- {-# INLINE liftE1flip #-}

-- liftE1 :: (a1 -> a2 -> c) -> a1 -> HQuery t a2 -> HQuery t c
-- liftE1 f = mapE . f
-- {-# INLINE liftE1 #-}

liftH :: (g a -> g b) -> HQuery f g a -> HQuery f g b
liftH f = HQuery . fmap f . unHQuery
{-# INLINE liftH #-}

liftE :: Functor f => (Vector a -> Vector b) -> Exp f a -> Exp f b
liftE = liftH . fmapComp

-- liftCWithName' :: (Maybe Symbol -> TableCol -> VectorH p) -> HQueryDyn f -> HQuery f p
-- liftCWithName' f = HQuery . liftCWithName f
-- {-# INLINABLE liftCWithName' #-}

-- mapED :: (forall a g. (Wrappable a, Wrappable1 g) => (g a) -> c) -> ExpDyn -> Exp c
-- mapED f = HQuery . (fromNamedTableReader_ . _2 . mapped %~ withWrapped (fmap f)) . unHQueryDyn
-- {-# INLINABLE mapED #-}

-- mapE :: (a -> b) -> HQuery t a -> HQuery t b
-- mapE = fmap
-- {-# INLINE mapE #-}

-- mapA1flip :: (Vector a -> x -> b) -> x -> Exp a -> Agg b
-- mapA1flip f = mapA . flip f
-- {-# INLINE mapA1flip #-}

-- mapA1 :: (x -> Vector a -> b) -> x -> Exp a -> Agg b
-- mapA1 f = mapA . f
-- {-# INLINE mapA1 #-}

-- mapA :: (Vector a -> b) -> Exp f a -> Agg f b
-- mapA f = withHQuery $ commonAgg $ \grps -> fmap2 f grps
-- {-# INLINE mapA #-}

-- mapD0 :: (forall a g. (Wrappable a, Wrappable1 g) => Vector (g a) -> b) -> ExpDyn -> Agg b
-- mapD0 f = HQuery . commonAgg (\grps -> withWrapped (fmap f . grps)) . unHQueryDyn
-- {-# INLINE mapD0 #-}

-- mapD1 :: (forall a g. (Wrappable a, Wrappable1 g) => Vector (g a) -> g a) -> ExpDyn -> AggDyn
-- mapD1 f = withHQueryDyn $ commonAgg $ \grps -> withWrapped' (\tr -> WrappedDyn tr . fmap f . grps)
-- {-# INLINE mapD1 #-}

-- commonAgg :: ((forall a . (Vector a -> Vector (Vector a))) -> a1 -> a2)
--   -> NamedTableReader Table a1 -> NamedTableReader GroupedTable a2
-- commonAgg f = fromNamedTableReader_ . _2 %~ \exp -> ReaderT $ \(GroupedTable _ grouper t) ->
--   f grouper <$> runReaderT exp t
-- {-# INLINABLE commonAgg #-}

fromMaybe_ :: Functor f => c -> Exp f (P.Maybe c) -> Exp f c
fromMaybe_ = fmap . P.fromMaybe
{-# INLINABLE fromMaybe_ #-}

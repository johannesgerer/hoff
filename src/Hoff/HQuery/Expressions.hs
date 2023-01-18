{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Hoff.HQuery.Expressions where

import           Hoff.Utils
import           Control.Lens hiding (Empty, (<.))
import           Control.Monad.Reader
import           Data.Coerce
import           Data.Text (unlines)
import           Data.SOP
import           Data.String
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Table as H
import           Yahp hiding (reader, ask, (:.:), group, delete, take, filter, (<|>), unlines)

type PrelimCols f = [(Maybe Symbol, GroupedCol f)]

newtype NamedTableReader f a = NamedTableReader
  { fromNamedTableReader :: (Maybe Symbol, ReaderT (GroupedTable f, PrelimCols f) H a) }
  deriving (Functor)

-- | f = Aggregation type: `I` (full table) or `Vector` (groups)
--   g = Group Result: `I` (aggregated) or `Vector` (homogenous)
newtype HQuery f  g a   = HQuery        { unHQuery      :: NamedTableReader f ((f :.: g) a) }
  deriving Functor 

newtype HQueryDyn f g   = HQueryDyn     { unHQueryDyn   :: NamedTableReader f (TableCol' (f :.: g)) }

makeLensesWithSuffix ''NamedTableReader
makeLensesWithSuffix ''HQueryDyn
makeLensesWithSuffix ''HQuery


-- * Regular (non-aggregate) expressions

type Exp f      = HQuery f Vector

type ExpDyn f   = HQueryDyn f Vector
type ExpDyns f  = [ExpDyn f]

-- * Aggregation expressions

type Agg f      = HQuery f I

type AggDyn f   = HQueryDyn f I
type AggDyns f  = [AggDyn f]

-- * get name
getName :: HQuery f g a -> Maybe Symbol
getName         = fst . fromNamedTableReader . unHQuery
{-# INLINE getName #-}

getNameDyn :: HQueryDyn f g -> Maybe Symbol
getNameDyn      = fst . fromNamedTableReader . unHQueryDyn
{-# INLINE getNameDyn #-}


-- * Sources for table columns / vectors


-- ** Table columns

-- | get static table column
cl :: (HasCallStack, ColumnData a f) => Symbol -> Exp f a
cl = tableCol
{-# INLINABLE cl #-}

-- | convert to maybe column
mc :: (HasCallStack, ColumnData a f) => Symbol -> Exp f (Maybe a)
mc n = liftC' (chain (fromWrappedDyn $ Just n) . toMaybe' n) $ tableColD n
{-# INLINABLE mc #-}

-- | see `tableColAgg`
aa :: (HasCallStack, ColumnData a f) => Symbol -> Agg f (Vector a)
aa = tableColAgg
{-# INLINABLE aa #-}

-- | get dynamic table column
ad :: HasCallStack => Symbol -> ExpDyn f
ad = tableColD

-- | explicitly renamed dynamic table column
ed :: HasCallStack => Symbol -> Symbol -> ExpDyn f
ed n = setName n . ad

-- | group number (zero-based)
rnA :: Agg f Int
rnA = noName $ reader gtGroupNumber

-- | row number (zero-based)
rn :: Exp f Int
rn = noName $ reader gtOriginalRowNumbers
{-# INLINABLE rn #-}

-- | row number in group (zero-based)
rnG :: Exp f Int
rnG = noName $ reader gtGroupRowNumbers

-- | unwrap I, leave other constructors untouched
tableCol :: (ColumnData a f, HasCallStack) => Symbol -> Exp f a
tableCol name = HQuery $ NamedTableReader $ (Just name, ReaderT g)
  where g (GroupedTable{..}, _) = wrapErrorMeta gtTable $
          gtToGroups "tableCol" =<< fromWrappedDyn (Just name) =<< getTableColWithoutWrapErrorMeta name gtTable
{-# INLINABLE tableCol #-}

-- | unwrap I, leave other constructors untouched
fromDyn :: (ColumnData a f, HasCallStack) => Maybe Symbol -> ExpDyn f -> Exp f a
fromDyn = liftC' . fromWrappedDyn
{-# INLINABLE fromDyn #-}

-- | access columns -- `Int`
tableColD :: HasCallStack => Symbol -> ExpDyn f
tableColD name = HQueryDyn $ NamedTableReader $ (Just name, ReaderT g)
  where g (GroupedTable{..}, _) = wrapErrorMeta gtTable $
          mapWrappedM (gtToGroups "tableColD") =<< getTableColWithoutWrapErrorMeta name gtTable
{-# INLINABLE tableColD #-}

getTableColWithoutWrapErrorMeta :: (HasCallStack, ToTable t) => Symbol -> t -> H TableCol
getTableColWithoutWrapErrorMeta name = chainToH $ \t -> flipTable t ! name
{-# INLINABLE getTableColWithoutWrapErrorMeta #-}

-- | like `tableCol` but this can access columns declared later in list of expression in `select` and
-- `update` family of functions
prelimCol :: (ColumnData a f, HasCallStack) => Symbol -> Exp f a
prelimCol n = fromDyn (Just n) $ prelimColD n
{-# INLINABLE prelimCol #-}

-- | like `tableColD` but this can access columns declared later in list of expression in `select` and
-- `update` family of functions
prelimColD :: HasCallStack => Symbol -> ExpDyn f
prelimColD name = HQueryDyn $ NamedTableReader $ (Just name, ReaderT g)
  where g (_, prelim) = noteH err $ lookup (Just name) prelim
          where err = KeyNotFound $ toS $ showt name <> "\n\n" <>
                  Data.Text.unlines (catMaybes $ fst <$> prelim)
{-# INLINE prelimColD #-}

-- -- | this cannot be done currectly in the current design because (I
-- -- None) columns need to remain castable to Maybe a for any a.
-- -- To achieve this, we would need to add Vector to the allowed constructors inside a Wrappable
-- tableColAggD :: (HasCallStack) => Symbol -> AggDyn
-- tableColAggD name = NamedTableReader (Just name, ReaderT $ \(t, GroupedTable _ grouper) ->
--                                          asd grouper <$> flipTable t ! name)
-- {-# INLINABLE tableColAggD #-}

-- asd :: (forall a . Vector a -> Vector (Vector a)) -> TableCol -> TableCol
-- asd grouper = \case
--   c@(WrappedDyn tr@(App con _) v)
--     | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> pure mempty
--     | Just HRefl <- con   `eqTypeRep` typeRep @I        -> fromWrappedDyn c
--     | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> fromWrappedDyn $ toWrappedDynI $ V.catMaybes v
--   w             -> errorMaybeOrI "n/a" w

-- | aggregates the values of the given static query for each group into a vector
--
-- see also `ca`
--
ae :: (HasCallStack, ColumnData a f) => Exp f a -> Agg f (Vector a)
ae = expVectorAgg
{-# INLINE ae #-}

-- | aggregates the values of the given static query for each group into a vector
expVectorAgg :: (HasCallStack, ColumnData a f) => Exp f a -> Agg f (Vector a)
expVectorAgg = mapA id
{-# INLINE expVectorAgg #-}

-- | aggregates the column values for each group into a vector
tableColAgg :: (HasCallStack, ColumnData a f) => Symbol -> Agg f (Vector a)
tableColAgg = expVectorAgg . tableCol
{-# INLINABLE tableColAgg #-}

-- *** Instances 

-- **** IsString
--
-- Without replacing the stock definition of fromString with one that HasCallStack (similar to
-- Yahp.fromLabel), this instance will not be very helpful. So the IsLabel instances are preferred.
-- 
-- instance IsString (ExpDyn f) where
--   fromString = tableColD . toS

-- instance (ColumnData a f) => IsString (Exp f a) where
--   fromString = tableCol . toS
--   {-# INLINABLE fromString #-}

-- -- | This is not a good idea, because it will lead to overlaps with [Char] = String!
-- instance {-# OVERLAPS #-} IsString [ExpDyn f] where
--   fromString = pure . tableColD . toS
  
-- instance ColumnData a f => IsString (Agg f (Vector a)) where
--   fromString = tableColAgg . toS
--   {-# INLINABLE fromString #-}

class HQueryColumn a where
  hc :: HasCallStack => Symbol -> a

instance {-# OVERLAPS #-} HQueryColumn Symbol where
  hc = id
  {-# INLINABLE hc #-}

instance {-# OVERLAPS #-} HQueryColumn (ExpDyns f) where
  hc s = [tableColD s]
  {-# INLINABLE hc #-}

instance {-# OVERLAPS #-} HQueryColumn (ExpDyn f) where
  hc = tableColD
  {-# INLINABLE hc #-}

instance {-# OVERLAPS #-} ColumnData a f => HQueryColumn (Exp f a) where
  hc = tableCol
  {-# INLINABLE hc #-}

instance {-# OVERLAPS #-} ColumnData a f => HQueryColumn (Agg f (Vector a)) where
  hc = tableColAgg
  {-# INLINABLE hc #-}

  
instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m (ExpDyns f) where
  fromLabel = hc $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m (ExpDyn f) where
  fromLabel = hc $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (ColumnData a f, KnownSymbol m) => IsLabel m (Exp f a) where
  fromLabel = hc $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (ColumnData a f, KnownSymbol m) => IsLabel m (Agg f (Vector a)) where
  fromLabel = hc $ fromLabel @m
  {-# INLINABLE fromLabel #-}

-- currently not supported? see tableColAggD
-- instance (KnownSymbol m) => IsLabel m AggDyn where
--   fromLabel :: HasCallStack => AggDyn
--   fromLabel = tableColAggD $ fromLabel @m
--   {-# INLINABLE fromLabel #-}

-- ** explicit vectors
--
-- most functions (like `select`) that consume these will check if the vector have the correct length

vec :: Vector a -> Exp f a
vec v = noName $ ReaderT $ \g -> gtToGroups g "vec" v
{-# INLINABLE vec #-}

-- | named vector with implicit `I a` type
vi :: (Wrappable a, ICoerce (f :.: Vector) a) => Symbol -> Vector a -> ExpDyn f
vi n = ei n . vec
{-# INLINE vi #-}

-- | named vector with explicit `f a` type
vf :: forall a g f . (Wrappable1 g, Wrappable a) => Symbol -> Vector (g a) -> ExpDyn f
vf n = ef n . vec
{-# INLINE vf #-}

vecA :: Vector a -> Agg Vector a
vecA v = noName $ reader $ const $ coerce v
{-# INLINABLE vecA #-}

-- ** explicit table colums

tcol :: TableCol' (f :.: g) -> HQueryDyn f g
tcol d = HQueryDyn $ NamedTableReader (Nothing, ReaderT $ \_ -> pure d)
{-# INLINABLE tcol #-}

tcoli :: Symbol -> TableCol' (f :.: g) -> HQueryDyn f g
tcoli n = sn n . tcol
{-# INLINABLE tcoli #-}

-- ** Constants

-- | named constant with `f a` value, e.g. `Maybe a` 
cm :: forall a h f g . (ICoerce (f :.: g) a, Wrappable a, ExpressionType f g, Wrappable1 h)
  => Symbol -> h a -> HQueryDyn f g
cm n = ef n . co
{-# INLINE cm #-}

-- | named constant with `I a` value
ci :: forall a f g . (ICoerce (f :.: g) a, Wrappable a, ExpressionType f g)
  => Symbol -> a -> HQueryDyn f g
ci n = ei n . co
{-# INLINE ci #-}

-- | constant static value
co :: forall a f g . (ExpressionType f g) => a -> HQuery f g a
co x = noName $ reader $ Comp . constColumnData x
{-# INLINE co #-}


-- ** Constants within group

-- | use aggregation result as constant value
--
-- see also `fby`, which allows independent grouping
--
-- see also `ae`
--
ca :: forall a f . (Zippable f, HasCallStack) => Agg f a -> Exp f a
ca = mapNTR $ \agg -> ReaderT $ \(g, prelim) -> deaggregate g <$> runReaderT agg (g, prelim)
{-# INLINE ca #-}

zipA :: forall a b c f . (Zippable f, HasCallStack) => (a -> b -> c) -> Agg f a -> Exp f b -> Exp f c
zipA f (HQuery agg) = mapHQuery (zipAHelper f agg <*>)
{-# INLINE zipA #-}

zipAHelper :: Zippable f => (a -> b -> c)
  -> NamedTableReader f ((f :.: I) a) -> NamedTableReader f ((f :.: Vector) b -> (f :.: Vector) c)
zipAHelper f = fmap $ mapComp . zipWith_ (fmap . f . unI) . unComp
{-# INLINE zipAHelper #-}

-- | collect

deaggregate :: Zippable f => GroupedTable f -> (f :.: I) a -> (f :.: Vector) a
deaggregate g = mapComp $ zipWith_ (\s (I v) -> V.replicate s v) $ gtGroupSizes g
{-# INLINABLE deaggregate #-}

type ExpressionTypeZip f g = (ExpressionType f g, Zippable f, Zippable g)
type Functor2 f g = (Functor f, Functor g)

class Functor f => ExpressionType f g where
  constColumnData :: a -> GroupedTable f -> f (g a)

class ExpressionTypeVector f g where
  mapVector :: (Vector a -> Vector b) -> (f :.: g) a -> (f :.: g) b
  mapHVector :: (Vector a -> VectorH b) -> (f :.: g) a -> H ((f :.: g) b)
  
instance Functor f => ExpressionType f I where
  constColumnData x g = I x <$ gtGroupSizes g
  {-# INLINE constColumnData #-}

instance Functor f => ExpressionType f Vector where
  constColumnData x g = flip V.replicate x <$> gtGroupSizes g
  {-# INLINE constColumnData #-}
  
instance ExpressionTypeVector Vector I where
  mapVector = coerce
  {-# INLINE mapVector #-}
  mapHVector = coerce
  {-# INLINE mapHVector #-}
  
instance ExpressionTypeVector Vector Vector where
  mapVector f = Comp . fmap f . unComp
  {-# INLINE mapVector #-}
  mapHVector f = fmap Comp . mapM f . unComp
  {-# INLINABLE mapHVector #-}
  
instance ExpressionTypeVector I Vector where
  mapVector = coerce
  {-# INLINE mapVector #-}
  mapHVector = coerce
  {-# INLINABLE mapHVector #-}


-- * Sinks

-- *** automatically named

-- | automatically named / `I a` value
ai :: (Wrappable a, ICoerce (f :.: g) a) => HQuery f g a -> HQueryDyn f g
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

-- *** explicitly named

-- | explicitly named / `I a` value
ei :: (Wrappable a, ICoerce (f :.: g) a) => Symbol -> HQuery f g a -> HQueryDyn f g
ei name = setName name . ai
{-# INLINE ei #-}

infix 3 </

-- | explicitly named / `I a` value
(</) :: (Wrappable a, ICoerce (f :.: g) a) => Symbol -> HQuery f g a -> HQueryDyn f g
(</) = ei
{-# INLINE (</) #-}

infix 3 <?
-- | explicitly named / `Maybe a` value
(<?) :: (Wrappable a) => Symbol -> HQuery f g (Maybe a) -> HQueryDyn f g
(<?) = em
{-# INLINE (<?) #-}


-- | explicitly named / `f a` value
ef :: forall h a f g . (Wrappable1 h, Wrappable a) => Symbol -> HQuery f g (h a) -> HQueryDyn f g
ef name = setName name . af
{-# INLINE ef #-}

-- | explicitly named / `Maybe a` value
em :: (Wrappable a) => Symbol -> HQuery f g (Maybe a) -> HQueryDyn f g
em = ef
{-# INLINE em #-}

                
sn :: Symbol -> HQueryDyn f g -> HQueryDyn f g
sn = setName
{-# INLINE sn #-}



-- * Helpers

noName :: ReaderT (GroupedTable f) H ((f :.: g) p) -> HQuery f g p
noName = HQuery . NamedTableReader . (Nothing,) . withReaderT fst
{-# INLINABLE noName #-}

withName :: Symbol -> ReaderT (GroupedTable f) H ((f :.: g) p) -> HQuery f g p
withName name = HQuery . NamedTableReader . (Just name,) . withReaderT fst
{-# INLINABLE withName #-}

nameLens :: (Maybe Symbol -> Identity (Maybe Symbol)) -> HQueryDyn f g -> Identity (HQueryDyn f g)
nameLens = unHQueryDyn_ . fromNamedTableReader_ . _1
{-# INLINABLE nameLens #-}

setName :: Symbol -> HQueryDyn f g -> HQueryDyn f g
setName name = nameLens .~ Just name
{-# INLINABLE setName #-}

mapNTR :: (ReaderT (GroupedTable f, PrelimCols f) H ((f :.: g) a)
  -> ReaderT (GroupedTable h, PrelimCols h) H ((h :.: k) b)) -> HQuery f g a -> HQuery h k b
mapNTR f = HQuery . NamedTableReader . second f . fromNamedTableReader . unHQuery
{-# INLINABLE mapNTR #-}

-- * Comp
mapComp :: (f (g a) -> h (k b)) -> (f :.: g) a -> (h :.: k) b
mapComp = coerce
{-# INLINE mapComp #-}

mapComp2 :: Functor f => (g a -> h b) -> (f :.: g) a -> (f :.: h) b
mapComp2 = mapComp . fmap 
{-# INLINE mapComp2 #-}

-- * NamedTableReader
mapNTR2 :: (a -> H b) -> NamedTableReader f a -> NamedTableReader f b
mapNTR2 f = fromNamedTableReader_ . _2 %~ mapReaderT (chain f)
{-# INLINABLE mapNTR2 #-}

-- * HQuery

mapHQuery :: (NamedTableReader f ((f :.: g) a) -> NamedTableReader h ((h :.: k) b))
  -> HQuery f g a -> HQuery h k b
mapHQuery = coerce
{-# INLINE mapHQuery #-}

mapHQueryCol :: (f (g a) -> f (h b)) -> HQuery f g a -> HQuery f h b
mapHQueryCol = mapHQuery . fmap . mapComp
{-# INLINE mapHQueryCol #-}

mapExp :: Functor f => (g a -> h b) -> HQuery f g a -> HQuery f h b
mapExp = mapHQueryCol . fmap
{-# INLINE mapExp #-}

mapHqDynStatic :: (NamedTableReader f (TableCol' (f :.: g)) -> NamedTableReader h ((h :.: k) a))
  -> HQueryDyn f g -> HQuery h k a
mapHqDynStatic f = HQuery . f . unHQueryDyn
{-# INLINE mapHqDynStatic #-}

mapHqDynStaticCol :: (TableCol' (h :.: g) -> (:.:) h k a) -> HQueryDyn h g -> HQuery h k a
mapHqDynStaticCol = mapHqDynStatic . fmap
{-# INLINE mapHqDynStaticCol #-}

liftC' :: (TableCol' (f :.: g) -> H ((f :.: g) p)) -> HQueryDyn f g -> HQuery f g p
liftC' =  mapHqDynStatic . mapNTR2
{-# INLINE liftC' #-}

-- * HQueryDyn

mapHQueryDyn :: (NamedTableReader f (TableCol' (f :.: g)) -> NamedTableReader h (TableCol' (h :.: k)))
  -> HQueryDyn f g -> HQueryDyn h k
mapHQueryDyn = coerce
{-# INLINE mapHQueryDyn #-}

liftC :: (TableCol' (f :.: g) -> TableColH' (f :.: k)) -> HQueryDyn f g -> HQueryDyn f k
liftC =  mapHQueryDyn . mapNTR2
{-# INLINE liftC #-}

mapHQueryDynCol :: (TableCol' (h :.: g) -> TableCol' (h :.: k)) -> HQueryDyn h g -> HQueryDyn h k
mapHQueryDynCol =  mapHQueryDyn . fmap

liftCWithName :: (Maybe Symbol -> TableCol' (f :.: g) -> H a) -> HQueryDyn f g -> NamedTableReader f a
liftCWithName f (HQueryDyn (NamedTableReader (n, r))) = NamedTableReader (n, mapReaderT (chain $ f n) r)
{-# INLINE liftCWithName #-}

liftCWithNameDyn :: (Maybe Symbol -> TableCol' (f :.: g1) -> H (TableCol' (f :.: g2)))
  -> HQueryDyn f g1 -> HQueryDyn f g2
liftCWithNameDyn f = HQueryDyn . liftCWithName f
{-# INLINABLE liftCWithNameDyn #-}

liftCWithNameStatic :: (Maybe Symbol -> TableCol' (f :.: g1) -> H ((f :.: g2) a))
  -> HQueryDyn f g1 -> HQuery f g2 a
liftCWithNameStatic f = HQuery . liftCWithName f
{-# INLINABLE liftCWithNameStatic #-}

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

liftVecH :: forall a b f g . ExpressionTypeVector f g
  => (Vector a -> VectorH b) -> HQuery f g a -> HQuery f g b
liftVecH = mapHQuery . mapNTR2 . mapHVector
{-# INLINE liftVecH #-}

liftVec :: forall a b f g . ExpressionTypeVector f g
  => (Vector a -> Vector b) -> HQuery f g a -> HQuery f g b
liftVec = mapHQuery . fmap . mapVector
{-# INLINE liftVec #-}

liftEH :: forall a b f . Traversable f => (Vector a -> VectorH b) -> Exp f a -> Exp f b
liftEH f = unHQuery_ . fromNamedTableReader_ . _2 %~ mapReaderT (chain $ coerce . mapM f . unComp)
{-# INLINE liftEH #-}

liftE1flip :: forall a c b f g . (Functor f, Functor g) => (a -> b -> c) -> b -> HQuery f g a -> HQuery f g c
liftE1flip = liftE1 . flip
{-# INLINE liftE1flip #-}

liftE1 :: forall b c a f g . (Functor f, Functor g) => (a -> b -> c) -> a -> HQuery f g b -> HQuery f g c
liftE1 f = mapE . f
{-# INLINE liftE1 #-}


-- mapED :: (forall a g. (Wrappable a, Wrappable1 g) => (g a) -> c) -> ExpDyn -> Exp c
-- mapED f = HQuery . (fromNamedTableReader_ . _2 . mapped %~ withWrapped (fmap f)) . unHQueryDyn
-- {-# INLINABLE mapED #-}

mapE :: forall a b f g . (Functor f, Functor g) => (a -> b) -> HQuery f g a -> HQuery f g b
mapE = fmap
{-# INLINE mapE #-}

mapA1flip :: forall a b x f . Functor f => (Vector a -> x -> b) -> x -> Exp f a -> Agg f b
mapA1flip f = mapA . flip f
{-# INLINE mapA1flip #-}

mapA1 :: forall a b x f . Functor f => (x -> Vector a -> b) -> x -> Exp f a -> Agg f b
mapA1 f = mapA . f
{-# INLINE mapA1 #-}

mapA :: forall a b f . Functor f => (Vector a -> b) -> Exp f a -> Agg f b
mapA f = mapExp (I . f) 
{-# INLINE mapA #-}

mapD0 :: forall b f . Functor f => (forall a g. (Wrappable a, Wrappable1 g) => Vector (g a) -> b)
  -> ExpDyn f -> Agg f b
mapD0 f = mapHqDynStatic $ fmap $ withWrapped $ mapComp2 (I . f)
{-# INLINE mapD0 #-}

mapD1 :: Functor f => (forall a g . (Wrappable2 g a) => Vector (g a) -> g a) -> ExpDyn f -> AggDyn f
mapD1 f = mapHQueryDynCol $ withWrapped' (\tr -> WrappedDyn tr . mapComp2 (I . f))
{-# INLINE mapD1 #-}


-- * Zippable

deriving instance Zippable H

instance Applicative (NamedTableReader f) where
  pure = NamedTableReader . (Nothing,) . pure
  (NamedTableReader (nf,f)) <*> (NamedTableReader (nx,x)) = NamedTableReader (nx `mplus` nf, f <*> x)
  {-# INLINABLE pure #-}

deriving via (ApplicativeZipper (NamedTableReader f)) instance Zippable (NamedTableReader f) 

instance (Zippable g, Zippable f) => ZippableHelper (HQuery f g) (NamedTableReader f) (f :.: g) where
  wrapAfterZip          = HQuery
  unwrapBeforeZip       = unHQuery

-- | if possible use the faster zipWith_, zipWith6_ etc
instance (Zippable f, Zippable g, ExpressionType f g) => Applicative (HQuery f g) where
  pure = co
  (<*>) = zipWith_ ($)
  {-# INLINE (<*>) #-}
  {-# INLINE pure #-}


instance (Functor f, Show (f Int), Show (f (Vector Int))) => Show (GroupedTable f) where
  show GroupedTable{..} = concat
    ["GroupedTable"
    ,"\n  { gtGroupSizes          = ", show gtGroupSizes
    ,"\n  , gtToGroups            = ", show $ fmap unComp $ gtToGroups "show" $ V.enumFromN @Int 0 $ count gtTable
    ,"\n  , gtOriginalRowNumbers  = ", show $ unComp gtOriginalRowNumbers
    ,"\n  , gtGroupRowNumbers     = ", show $ unComp gtOriginalRowNumbers
    ,"\n  , gtGroupNumber         = ", show $ unI <$> unComp gtGroupNumber
    ,"\n  , gtTable               = \n", show gtTable
    ]


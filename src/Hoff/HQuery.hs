{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Hoff.HQuery where

import           Control.Lens hiding (Empty, (<.))
import           Control.Monad.Reader
import           Data.Coerce
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import           Data.String
import           Data.Vector (Vector)
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
  { fromNamedTableReader :: (Maybe Symbol, ReaderT f H a) }
  deriving (Functor)

newtype HQuery f a      = HQuery        { unHQuery      :: NamedTableReader f (Vector a) }
  deriving Functor

newtype HQueryDyn f     = HQueryDyn     { unHQueryDyn   :: NamedTableReader f TableCol }

data GroupedTable = GroupedTable { gtGroupSizes :: Vector Int
                                 , gtToGroups   :: forall a . Vector a -> Vector (Vector a)
                                 -- ^ takes the source vector and returns the groups
                                 , gtTable      :: Table
                                 }

-- ** Regular expressions

type Exp        = HQuery Table

type ExpDyn     = HQueryDyn Table
type ExpDyns    = [ExpDyn]

-- ** Aggregation expressions

type Agg        = HQuery GroupedTable

type AggDyn     = HQueryDyn GroupedTable
type AggDyns    = [AggDyn]


-- besser: 
data GroupedTable' f = GroupedTable' { gtGroupSizes :: Vector Int
                                    , gtToGroups   :: forall a . Vector a -> Vector (Vector a)
                                    -- ^ takes the source vector and returns the groups
                                    , gtTable      :: Table
                                    , gtGrouped    :: VectorDict Symbol (WrappedDyn (f :.: Vector))
                                    }

type Exp       a = (GroupedTable' f) -> f (Vector a)
type ExpDyn      = (GroupedTable' f) -> f (Vector TableCol)

type Agg       a = (GroupedTable' f) -> f a
type AggDyn    a = (GroupedTable' f) -> f TableCol



makeLensesWithSuffix ''NamedTableReader
makeLensesWithSuffix ''HQueryDyn
makeLensesWithSuffix ''HQuery

class HQueryContext a where
  contextCount :: a -> Int

instance HQueryContext Table where
  contextCount = count
  {-# INLINE contextCount #-}

instance HQueryContext GroupedTable where
  contextCount = count . gtGroupSizes
  {-# INLINE contextCount #-}

instance Applicative (NamedTableReader f) where
  pure = NamedTableReader . (Nothing,) . pure
  (NamedTableReader (nf,f)) <*> (NamedTableReader (nx,x)) = NamedTableReader (nx `mplus` nf, f <*> x)
  {-# INLINABLE pure #-}

fromGrouping :: Grouping -> Table -> GroupedTable
fromGrouping i = GroupedTable (V.length <$> i) $ applyGrouping i

fullGroup :: Table -> GroupedTable
fullGroup t = GroupedTable (pure $ count t) pure t
{-# INLINABLE fullGroup #-}

instance {-# OVERLAPS #-} KnownSymbol m => IsLabel m [Symbol] where
  fromLabel = pure $ fromLabel @m
  {-# INLINABLE fromLabel #-}
  
instance {-# OVERLAPS #-} KnownSymbol m => IsLabel m Symbols where
  fromLabel = pure $ fromLabel @m
  {-# INLINABLE fromLabel #-}
  
instance {-# OVERLAPS #-} KnownSymbol m => IsLabel m Symbol where
  fromLabel = toS $ symbolVal (Proxy @m)
  {-# INLINABLE fromLabel #-}
  
-- * Query types and construction

noName :: ReaderT f H (Vector p) -> HQuery f p
noName = HQuery . NamedTableReader . (Nothing,)
{-# INLINABLE noName #-}

withName :: Symbol -> ReaderT f H (Vector p) -> HQuery f p
withName name = HQuery . NamedTableReader . (Just name,)
{-# INLINABLE withName #-}

nameLens :: (Maybe Symbol -> Identity (Maybe Symbol)) -> HQueryDyn f -> Identity (HQueryDyn f)
nameLens = unHQueryDyn_ . fromNamedTableReader_ . _1
{-# INLINABLE nameLens #-}

setName :: Symbol -> HQueryDyn f -> HQueryDyn f
setName name = nameLens .~ Just name
{-# INLINABLE setName #-}

mapNTR :: (ReaderT f H (Vector a1) -> ReaderT g H (Vector a2)) -> HQuery f a1 -> HQuery g a2
mapNTR f = HQuery . NamedTableReader . second f . fromNamedTableReader . unHQuery
{-# INLINABLE mapNTR #-}

mapNTR2 :: (a -> H b) -> NamedTableReader f a -> NamedTableReader f b
mapNTR2 f = fromNamedTableReader_ . _2 %~ mapReaderT (chain f)
{-# INLINABLE mapNTR2 #-}

withHQuery f = HQuery . f . unHQuery

withHQueryDyn f = HQueryDyn . f . unHQueryDyn
withHqDynStatic f = HQuery . f . unHQueryDyn

runForgetName :: HQueryDyn f -> f -> H TableCol
runForgetName = runReaderT . snd . fromNamedTableReader. unHQueryDyn
{-# INLINABLE runForgetName #-}

runForgetNameNameC :: HQuery f p -> f -> H (Vector p)
runForgetNameNameC = runReaderT . snd . fromNamedTableReader . unHQuery
{-# INLINABLE runForgetNameNameC #-}

-- ** Exp 

-- *** automatically named

-- | automatically named / `I a` value
ai :: (Wrappable a) => HQuery f a -> HQueryDyn f
ai = HQueryDyn . fmap toWrappedDynI . unHQuery
{-# INLINE ai #-}

-- | automatically named / `Maybe a` value
am :: (Wrappable a) => HQuery f (Maybe a) -> HQueryDyn f
am = af
{-# INLINE am #-}

-- | automatically named / `f a` value
af :: forall g a f . (Wrappable a, Wrappable1 g) => HQuery f (g a) -> HQueryDyn f
af = HQueryDyn . fmap toWrappedDyn . unHQuery
{-# INLINE af #-}

-- *** explicitly named

-- | explicitly named / `I a` value
ei :: (Wrappable a) => Symbol -> HQuery f a -> HQueryDyn f
ei name = setName name . ai
{-# INLINE ei #-}

infix 3 </

-- | explicitly named / `I a` value
(</) :: (HasCallStack, Wrappable a) => Symbol -> HQuery f a -> HQueryDyn f
(</) = ei
{-# INLINE (</) #-}

infix 3 <?
-- | explicitly named / `Maybe a` value
(<?) :: (Wrappable a) => Symbol -> HQuery f (Maybe a) -> HQueryDyn f
(<?) = em
{-# INLINE (<?) #-}


-- | explicitly named / `f a` value
ef :: forall g a f . (Wrappable1 g, Wrappable a) => Symbol -> HQuery f (g a) -> HQueryDyn f
ef name = setName name . af
{-# INLINE ef #-}

em :: (Wrappable a) => Symbol -> HQuery f (Maybe a) -> HQueryDyn f
em = ef
{-# INLINE em #-}

                
sn :: Symbol -> HQueryDyn f -> HQueryDyn f
sn = setName
{-# INLINE sn #-}

-- ** Exp sources

-- *** Constants

-- | named constant with `f a` value, e.g. `Maybe a` 
cm :: forall a g f . (HQueryContext f, Wrappable1 g, Wrappable a) => Symbol -> g a -> HQueryDyn f
cm n = ef n . co
{-# INLINE cm #-}

-- | named constant with `I a` value
ci :: forall a f . (HQueryContext f, Wrappable a) => Symbol -> a -> HQueryDyn f
ci n = ei n . co
{-# INLINE ci #-}

-- | constant static value
co :: forall a f . HQueryContext f => a -> HQuery f a
co x = noName $ reader $ flip V.replicate x . contextCount
{-# INLINE co #-}

-- | use aggregation result as constant value
ca :: HasCallStack => Agg a -> Exp a
ca = mapNTR $ \agr -> ReaderT $ \t -> replicateSameLength t . V.head <$> runReaderT agr (fullGroup t)
{-# INLINE ca #-}


-- | for each group, distribute the aggregated value back to the whole group
fby :: HasCallStack => Agg a -> ExpDyns -> Exp a
fby agg bys = flip mapNTR agg $ \agg' -> ReaderT $ \t -> do
  idxs <- snd . getGroupsAndGrouping <$> select bys t
  broadcastGroupValue idxs <$> runReaderT agg' (fromGrouping idxs t)
{-# INLINABLE fby #-}

-- * Window functions

-- | for each group, generate a new vector with one entry for each row in the group
aggVectorDistributer :: HasCallStack => Agg (Vector a) -> ExpDyns -> Exp a
aggVectorDistributer agg bys = flip mapNTR agg $ \agg' -> ReaderT $ \t -> do
  idxs <- snd . getGroupsAndGrouping <$> select bys t
  unsafeBackpermute (V.concatMap id idxs) . V.concatMap id <$> runReaderT agg' (fromGrouping idxs t)
{-# INLINABLE aggVectorDistributer #-}

-- | for each group, generate the resulting vector and distribute it back to the original row
-- for functions that do not depend on the order of the input, we have `eby = const`
--
-- the function must not change the length of the vector
eby :: (HasCallStack, Typeable a) => (Vector a -> Vector b) -> ExpDyns -> Exp a -> Exp b
eby f bys exp' = flip mapNTR exp' $ \exp -> ReaderT $ \t -> do
  idxs <- snd . getGroupsAndGrouping <$> select bys t
  expVs <- applyGrouping idxs <$> runReaderT exp t
  let resVs = f <$> expVs
  if fmap length expVs == fmap length resVs then
    pure $ unsafeBackpermute (V.concatMap id idxs) $ V.concatMap id resVs
    else throwH $ CountMismatch $ unlines $ "Function returned vector of different length:\n" :
         toList (V.zipWith (\a b -> show (length a, length b)) expVs (V.take 10 resVs))
{-# INLINE eby #-}


nextD :: HasCallStack => HQueryDyn t -> HQueryDyn t
nextD = HQueryDyn . liftCWithName (modifyMaybeCol $ Just $ TableColModifierWithDefault nextV)
{-# INLINABLE nextD #-}

prevD :: HasCallStack => HQueryDyn t -> HQueryDyn t
prevD = HQueryDyn . liftCWithName (modifyMaybeCol $ Just $ TableColModifierWithDefault prevV)
{-# INLINABLE prevD #-}

-- nextD :: HQueryDyn t -> HQueryDyn t
-- nextD = liftE $ flip V.snoc Nothing . V.tail

prev :: forall a t . HQuery t a -> HQuery t (Maybe a)
prev = liftE $ prevV Nothing . fmap Just
{-# INLINABLE prev #-}

next :: forall a t . HQuery t a -> HQuery t (Maybe a)
next = liftE $ nextV Nothing . fmap Just
{-# INLINABLE next #-}

prevM :: forall a t . HQuery t (Maybe a) -> HQuery t (Maybe a)
prevM = liftE $ prevV Nothing
{-# INLINABLE prevM #-}

nextM :: forall a t . HQuery t (Maybe a) -> HQuery t (Maybe a)
nextM = liftE $ nextV Nothing
{-# INLINABLE nextM #-}

prevV :: a -> Vector a -> Vector a
prevV d = V.cons d . V.init
{-# INLINABLE prevV #-}

nextV :: b -> Vector b -> Vector b
nextV d = flip V.snoc d . V.tail
{-# INLINABLE nextV #-}
  
-- *** explicit vectors
--
-- most functions (like `select`) that consume these will check if the vector have the correct length

vec :: Vector a -> HQuery f a
vec v = noName $ reader $ const v


-- | named vector with implicit `I a` type
vi :: Wrappable a => Symbol -> Vector a -> HQueryDyn f
vi n = ei n . vec

-- | named vector with explicit `f a` type
vf :: forall a g f . (Wrappable1 g, Wrappable a) => Symbol -> Vector (g a) -> HQueryDyn f
vf n = ef n . vec

-- *** Table columns

-- | get static table column
ac :: (HasCallStack, Typeable a) => Symbol -> Exp a
ac = tableCol
{-# INLINABLE ac #-}

-- | convert to maybe column
mc :: (HasCallStack, Typeable a) => Symbol -> Exp (Maybe a)
mc n = liftC' (chain (fromWrappedDyn $ Just n) . toMaybe' n) $ tableColD n
{-# INLINABLE mc #-}

-- | see `tableColAgg`
aa :: (HasCallStack, Typeable a) => Symbol -> Agg (Vector a)
aa = tableColAgg
{-# INLINABLE aa #-}

-- | get dynamic table column
ad :: HasCallStack => Symbol -> ExpDyn
ad = tableColD

-- | explicitly renamed dynamic table column
ed :: HasCallStack => Symbol -> Symbol -> ExpDyn
ed n = setName n . ad

-- | unwrap I, leave other constructors untouched
tableCol :: (HasCallStack, Typeable a) => Symbol -> Exp a
tableCol n = liftC' (fromWrappedDyn $ Just n) $ tableColD n
{-# INLINABLE tableCol #-}

-- | access columns and the virtual column `i` containig row number / row index (zero-based) of type
-- `Int`
tableColD :: HasCallStack => Symbol -> ExpDyn
tableColD name = HQueryDyn $ NamedTableReader $ if name == "i"
  then (Nothing, ReaderT $ pure . toWrappedDynI . unsafeTableRowNumbers)
  else (Just name
       ,ReaderT $ \t -> mapHException (appendMsg $ "\n\n" <> show (meta t)) $ flipTable t ! name)
{-# INLINE tableColD #-}

-- test :: HasCallStack => Exp Int
-- test = noName $ ReaderT $ \t -> fromWrappedDyn =<< flipTable (fst t) ! "asd"

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
expVectorAgg :: (HasCallStack, Typeable a) => Exp a -> Agg (Vector a)
expVectorAgg = mapA id
{-# INLINE expVectorAgg #-}

-- | aggregates the column values for each group into a vector
tableColAgg :: (HasCallStack, Typeable a) => Symbol -> Agg (Vector a)
tableColAgg = expVectorAgg . tableCol
{-# INLINABLE tableColAgg #-}

instance IsString ExpDyn where
  fromString :: HasCallStack => String -> ExpDyn
  fromString = tableColD . toS
  
instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m ExpDyns where
  fromLabel :: HasCallStack => ExpDyns
  fromLabel = [fromLabel @m]
  {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m (ExpDyn) where
  fromLabel :: HasCallStack => ExpDyn
  fromLabel = tableColD $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance Typeable a => IsString (Exp a) where
  fromString :: HasCallStack => String -> Exp a
  fromString = tableCol . toS
  {-# INLINABLE fromString #-}
  
instance {-# OVERLAPS #-} (Typeable a, KnownSymbol m) => IsLabel m (Exp a) where
  fromLabel :: HasCallStack => Exp a
  fromLabel = tableCol $ fromLabel @m
  {-# INLINABLE fromLabel #-}
  
-- instance (KnownSymbol m) => IsLabel m AggDyn where
--   fromLabel :: HasCallStack => AggDyn
--   fromLabel = tableColAggD $ fromLabel @m
--   {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (Typeable a, KnownSymbol m) => IsLabel m (Agg (Vector a)) where
  fromLabel :: HasCallStack => Agg (Vector a)
  fromLabel = tableColAgg $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance Typeable a => IsString (Agg (Vector a)) where
  fromString = tableColAgg . toS
  {-# INLINABLE fromString #-}
  

-- * Query execution


type Where = Exp Bool

runWhere :: ToTable t => Where -> t -> H WhereResult
runWhere wh = chainToH @Table $ \t -> fromMask <$> runForgetNameNameC wh t
{-# INLINABLE runWhere #-}

filter :: (HasCallStack, ToTableAndBack t) => Where -> t -> H' t
filter wh = useToTableAndBack $ \t -> chain2 applyWhereResult t $ runWhere wh t
{-# INLINABLE filter #-}

-- * Helper function

toFiltered :: (HasCallStack, ToTable t) => (a -> Table -> H c) -> a -> Where -> t -> H c
toFiltered x a wh = chainToH @Table $ x a <=< filter wh 
{-# INLINE toFiltered #-}

toFiltered2 :: (HasCallStack, ToTable t) => (t1 -> t2 -> Table -> H c) -> t1 -> t2 -> Where -> t -> H c
toFiltered2 x a b wh = chainToH @Table $ x a b <=< filter wh 
{-# INLINE toFiltered2 #-}

toFiltered3 :: (HasCallStack, ToTable t) => (t1 -> t2 -> t3 -> Table -> H c) -> t1 -> t2 -> t3 -> Where -> t -> H c
toFiltered3 x a b c wh = chainToH @Table $ x a b c <=< filter wh 
{-# INLINE toFiltered3 #-}



-- ** delete

deleteW :: (HasCallStack, ToTableAndBack t) => Where -> t -> H' t
deleteW = useToTableAndBack . filter . fmap not
{-# INLINABLE deleteW #-}

delete :: (HasCallStack, ToTableAndBack t, ToVector f) => f Symbol -> t -> H' t
delete s = useToTableAndBack $ chainToH $ tableNoLengthCheck . H.deleteKeys (toVector s) . flipTable
{-# INLINABLE delete #-}

-- ** select
--
-- todo:
-- select[n]
-- select[m n]
-- select[order]
-- select[n;order]
-- select distinct

select :: (ToTableAndBack t, HasCallStack) => ExpDyns -> t -> H' t
{-# INLINABLE select #-}
select e = useToTableAndBack $ chainToH $ \t -> case e of
  [] -> toH t
  e  -> tableAnonymous =<< mapM (mapM (flip runReaderT t) . fromNamedTableReader . unHQueryDyn) e

selectW :: (ToTableAndBack t, HasCallStack) => ExpDyns -> Where -> t -> H' t
selectW e = useToTableAndBack . toFiltered select e
{-# INLINE selectW #-}

selectBy :: (ToTable t, HasCallStack) => AggDyns -> ExpDyns -> t -> KeyedTableH
selectBy aggs bys t = uncurry dictNoLengthCheck . snd <$> selectByRaw' aggs bys (toH t)
{-# INLINABLE selectBy #-}

selectByRaw' :: HasCallStack => AggDyns -> ExpDyns -> TableH -> H (Grouping, (Table, Table))
selectByRaw' aggs bys t = select bys t >>= \bys' -> traverse2 tableAnonymous =<< selectByRaw aggs bys' t
{-# INLINABLE selectByRaw' #-}

selectByRaw :: (ToTable tt, HasCallStack) => (HashableDictComponent t, Traversable f)
  => f AggDyn -> t -> tt -> H (Grouping, (t, (f (Maybe Symbol, TableCol))))
selectByRaw aggs by = chainToH $ \t -> (idxs,) <$> (keys,)
  <$> forM aggs (mapM (flip runReaderT (fromGrouping idxs t)) . fromNamedTableReader . unHQueryDyn)
  where (keys, idxs) = getGroupsAndGrouping by
{-# INLINABLE selectByRaw #-}

firstRowPerGroup :: ToH Table t => ExpDyns -> t -> KeyedTableH
firstRowPerGroup groupBy = chainToH @Table g
  where g t = do
          groupBy' <- select groupBy t
          (key, I (_, idxs)) <- snd <$> selectByRaw (I $ ai $ coerce @Int @IterIndex . V.head <$> #i) groupBy' t
          idxs' <- fromWrappedDyn Nothing idxs
          dictNoLengthCheck key . unsafeBackpermute idxs' <$> delete (cols key) t
{-# INLINABLE firstRowPerGroup #-}


selectByW :: (ToTable t, HasCallStack) => AggDyns -> ExpDyns -> Where -> t -> KeyedTableH
selectByW = toFiltered2 selectBy
{-# INLINE selectByW #-}

selectAgg :: (ToTable t, HasCallStack) => AggDyns -> t -> TableH
selectAgg aggs = chainToH $ \t -> tableAnonymous
  =<< forM aggs (mapM (flip runReaderT (fullGroup t)) . fromNamedTableReader . unHQueryDyn)
{-# INLINABLE selectAgg #-}

selectAggW :: (ToTable t, HasCallStack) => AggDyns -> Where -> t -> TableH
selectAggW = toFiltered selectAgg
{-# INLINE selectAggW #-}


-- ** update

update :: (ToTableAndBack t, HasCallStack) => ExpDyns -> t -> H' t
update e = useToTableAndBack $ \t -> mergeTablesPreferSecond t =<< select e t
{-# INLINABLE update #-}

-- | new columns are converted to Maybe (if they are not already) to be able to represent missing value
-- that could appear based on data
updateW :: (ToTable t, HasCallStack) => ExpDyns -> Where -> t -> TableH
{-# INLINABLE updateW #-}
updateW e wh = chainToH $ \t -> runWhere wh t >>= withWhereResult (update e) pure subset t
  where subset mask _ table = 
          let pick name (WrappedDyn t new) = fmap2 (WrappedDyn t . conditional mask new)
                $ fromWrappedDyn $ Just name
              sparseNew _ (WrappedDyn (App con _) col)
                | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = pure $ toWrappedDyn $ V.zipWith
                  (\m n -> if m then n else Nothing) mask col
                | Just HRefl <- con `eqTypeRep` R.typeRep @I          = pure $ toWrappedDyn $ V.zipWith
                  (\m n -> if m then Just n else Nothing) mask $ coerceUI col
              sparseNew name          w                             = errorMaybeOrI (Just name) w
          in flip (mergeTablesWith sparseNew untouchedA pick) table =<< select e table


updateBy :: (ToTableAndBack t, HasCallStack) => AggDyns -> ExpDyns -> t -> H' t
updateBy aggs bys = useToTableAndBack $ \t -> selectByRaw' aggs bys t >>= \(idxs, (_, aggTable)) ->
  mergeTablesPreferSecond t (broadcastGroupValue idxs aggTable)
{-# INLINABLE updateBy #-}

-- ** exec

execD :: (ToTable t, HasCallStack) => ExpDyn -> t -> TableColH
execD = chainToH . runForgetName
{-# INLINE execD #-}

execDW :: (ToTable t, HasCallStack) => ExpDyn -> Where -> t -> TableColH
execDW = toFiltered execD
{-# INLINE execDW #-}

exec :: forall a t . (ToTable t, HasCallStack) => Exp a -> t -> VectorH a
exec = chainToH . runForgetNameNameC
{-# INLINE exec #-}

execW :: forall a t . (ToTable t, HasCallStack) => Exp a -> Where -> t -> VectorH a
execW = toFiltered exec
{-# INLINE execW #-}


execC :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Exp r -> t -> TypedTableH r
execC es = chainToH $ \t -> UnsafeTypedTable <$> R.mapM (\e -> runForgetNameNameC e t) es
{-# INLINABLE execC #-}

execCW :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Exp r -> Where -> t -> TypedTableH r
execCW = toFiltered execC
{-# INLINE execCW #-}

-- | map to row
execRBy :: forall g r t . (ToTable t, HasCallStack, TableRow r, Wrappable g)
        => Record Agg r -> Exp g -> t -> VectorDictH g (Record I r)
execRBy aggs by = chainToH $ \t -> (getGroupsAndGrouping <$> exec by t) >>= \(keys, idxs) ->
  dictNoLengthCheck keys . flipTypedTable I . UnsafeTypedTable
  <$> R.mapM (flip runForgetNameNameC (fromGrouping idxs t)) aggs
{-# INLINABLE execRBy #-}

execRByW :: forall g r t . (ToTable t, HasCallStack, TableRow r, Wrappable g)
        => Record Agg r -> Exp g -> Where -> t -> VectorDictH g (Record I r)
execRByW = toFiltered2 execRBy
{-# INLINE execRByW #-}

execRAgg :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Agg r -> t -> H (Record I r)
execRAgg aggs = chainToH $ \t -> R.mapM (fmap (I . V.head) . flip runForgetNameNameC (fullGroup t)) aggs
{-# INLINABLE execRAgg #-}

execRAggW :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Agg r -> Where -> t -> H (Record I r)
execRAggW = toFiltered execRAgg
{-# INLINABLE execRAggW #-}

-- | map
execMap :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Exp v -> Exp g -> t -> VectorDictH g v
execMap va ke t = dictNoLengthCheck <$> exec ke t <*> exec va t
{-# INLINABLE execMap #-}

execKey :: forall v g t . (ToKeyedTable t, HasCallStack, Wrappable g, Wrappable v) => Exp v -> Exp g -> t -> VectorDictH g v
execKey va ke = chainToH @KeyedTable $ \kt -> dictNoLengthCheck <$> exec ke (key kt) <*> exec va (value kt)
{-# INLINABLE execKey #-}

execBy :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Agg v -> Exp g -> t -> VectorDictH g v
execBy agg by t = do
  bys <- exec by t
  (_, (key, val)) <- selectByRaw (I $ ai agg) bys t
  dictNoLengthCheck key <$> fromWrappedDyn Nothing (snd $ unI val)
{-# INLINABLE execBy #-}

execByW :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Agg v -> Exp g -> Where -> t -> VectorDictH g v
execByW = toFiltered2 execBy
{-# INLINE execByW #-}

execAgg :: forall a t . (ToTable t, HasCallStack) => Agg a -> t -> H a
execAgg ag = chainToH $ fmap V.head . runForgetNameNameC ag . fullGroup
{-# INLINABLE execAgg #-}

execAggW :: forall a t . (ToTable t, HasCallStack) => Agg a -> Where -> t -> H a
execAggW = toFiltered execAgg
{-# INLINABLE execAggW #-}


-- -- fmapWithErrorContext :: (a -> b) -> Exp a -> Exp b
-- -- fmapWithErrorContext f = V.imap (\i a -> f a

-- -- fromJust :: Exp (Maybe a) -> Exp a
-- -- fromJust me = ReaderT $ \t -> f t $ runReaderT me t
-- --   where f table mE = let res = V.catMaybes mE
-- --           in if length res == length mE then res
-- --           else do throwH $ UnexpectedNulls $ "\n" <> show (unsafeBackpermute (IterIndex <$> V.findIndices isNothing mE) table)
  

-- * Sorting (unstable)

sortT :: forall e t . (HasCallStack, ToTable t) => Exp e -> Comparison e -> t -> TableH
sortT w by = chainToH $ sortByWithM (runForgetNameNameC w) by
{-# INLINABLE sortT #-}

ascT :: forall e t . (HasCallStack, ToTable t, Eq e, Ord e) => Exp e -> t -> TableH
ascT w = sortT w compare
{-# INLINE ascT #-}

descT :: forall e t . (HasCallStack, ToTable t, Eq e, Ord e) => Exp e -> t -> TableH
descT w = sortT w $ flip compare
{-# INLINE descT #-}

sortTD :: (HasCallStack, ToTable t) => ExpDyn -> (forall f a . (Ord1 f, Ord a) => f a -> f a -> Ordering) -> t -> TableH
sortTD w by = chainToH $ \t -> withWrapped (\v -> sortByWith (const v) by t) <$> runForgetName w t
{-# INLINABLE sortTD #-}

ascTD :: (HasCallStack, ToTable t) => ExpDyn -> t -> TableH
ascTD w = sortTD w compare1
{-# INLINABLE ascTD #-}

descTD :: (HasCallStack, ToTable t) => ExpDyn -> t -> TableH
descTD w = sortTD w $ flip compare1
{-# INLINABLE descTD #-}

ascC :: TableCol -> TableCol
ascC = mapWrapped (sortByWith id compare1)

descC :: TableCol -> TableCol
descC = mapWrapped (sortByWith id $ flip compare1)




-- * operator constructors


liftC :: (TableCol -> H TableCol) -> HQueryDyn f -> HQueryDyn f
liftC =  withHQueryDyn . mapNTR2
{-# INLINABLE liftC #-}

liftC' :: (TableCol -> H (Vector p)) -> HQueryDyn f -> HQuery f p
liftC' =  withHqDynStatic . mapNTR2
{-# INLINABLE liftC' #-}

liftCWithName :: (Maybe Symbol -> TableCol -> H a) -> HQueryDyn f -> NamedTableReader f a
liftCWithName f (HQueryDyn (NamedTableReader (n, r))) = NamedTableReader . (n,) $ mapReaderT (chain $ f n) r
{-# INLINE liftCWithName #-}

liftE6 :: forall a b c d e f x t .
  (Vector a -> Vector b -> Vector c -> Vector d -> Vector e -> Vector f -> Vector x)
  -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e -> HQuery t f -> HQuery t x
liftE6 f = coerce $ liftA6 @(NamedTableReader t) f
{-# INLINE liftE6 #-}

liftE5 :: forall a b c d e x t . (Vector a -> Vector b -> Vector c -> Vector d -> Vector e -> Vector x)
  -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e -> HQuery t x
liftE5 f = coerce $ liftA5 @(NamedTableReader t) f
{-# INLINE liftE5 #-}

liftE4 :: forall a b c d x t . (Vector a -> Vector b -> Vector c -> Vector d -> Vector x)
  -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t x
liftE4 f = coerce $ liftA4 @(NamedTableReader t) f
{-# INLINE liftE4 #-}

liftE3 :: forall a b c x t .
  (Vector a -> Vector b -> Vector c -> Vector x) -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t x
liftE3 f = coerce $ liftA3 @(NamedTableReader t) f
{-# INLINE liftE3 #-}

liftE2 :: forall a b c t . (Vector a -> Vector b -> Vector c) -> HQuery t a -> HQuery t b -> HQuery t c
liftE2 f = coerce $ liftA2 @(NamedTableReader t) f
{-# INLINE liftE2 #-}

liftE :: (Vector a -> Vector b) -> HQuery t a -> HQuery t b
liftE f = HQuery . fmap f . unHQuery
{-# INLINE liftE #-}

liftEH :: (Vector a -> VectorH b) -> HQuery t a -> HQuery t b
liftEH f = unHQuery_ . fromNamedTableReader_ . _2 %~ mapReaderT (chain f)
{-# INLINE liftEH #-}

liftCWithName' :: (Maybe Symbol -> TableCol -> VectorH p) -> HQueryDyn f -> HQuery f p
liftCWithName' f = HQuery . liftCWithName f
{-# INLINABLE liftCWithName' #-}

mapED :: (forall a g. (Wrappable a, Wrappable1 g) => (g a) -> c) -> ExpDyn -> Exp c
mapED f = HQuery . (fromNamedTableReader_ . _2 . mapped %~ withWrapped (fmap f)) . unHQueryDyn
{-# INLINABLE mapED #-}

mapE :: (a -> b) -> HQuery t a -> HQuery t b
mapE = fmap
{-# INLINE mapE #-}

mapA1flip :: (Vector a -> x -> b) -> x -> Exp a -> Agg b
mapA1flip f = mapA . flip f
{-# INLINE mapA1flip #-}

mapA1 :: (x -> Vector a -> b) -> x -> Exp a -> Agg b
mapA1 f = mapA . f
{-# INLINE mapA1 #-}

mapA :: (Vector a -> b) -> Exp a -> Agg b
mapA f = withHQuery $ commonAgg $ \grps -> fmap2 f grps
{-# INLINE mapA #-}

mapD0 :: (forall a g. (Wrappable a, Wrappable1 g) => Vector (g a) -> b) -> ExpDyn -> Agg b
mapD0 f = HQuery . commonAgg (\grps -> withWrapped (fmap f . grps)) . unHQueryDyn
{-# INLINE mapD0 #-}

mapD1 :: (forall a g. (Wrappable a, Wrappable1 g) => Vector (g a) -> g a) -> ExpDyn -> AggDyn
mapD1 f = withHQueryDyn $ commonAgg $ \grps -> withWrapped' (\tr -> WrappedDyn tr . fmap f . grps)
{-# INLINE mapD1 #-}

commonAgg :: ((forall a . (Vector a -> Vector (Vector a))) -> a1 -> a2)
  -> NamedTableReader Table a1 -> NamedTableReader GroupedTable a2
commonAgg f = fromNamedTableReader_ . _2 %~ \exp -> ReaderT $ \(GroupedTable _ grouper t) ->
  f grouper <$> runReaderT exp t
{-# INLINABLE commonAgg #-}

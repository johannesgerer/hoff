module Hoff.HQuery.Execution where

import           Control.Lens hiding (Empty, (<.))
import           Control.Monad.Reader
import           Data.Coerce
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.HQuery.Expressions
import           Hoff.Table as H
import           Hoff.TypedTable
import           Hoff.Vector
import           Type.Reflection as R
import           Yahp hiding (filter, (:.:), sortBy)

-- * Grouped Table

fromGrouping :: Grouping -> Table -> GroupedTable Vector
fromGrouping g t = GroupedTable
  { gtGroupSizes        = sizes
  , gtToGroups          = fmap2 Comp . applyGrouping g
  , gtOriginalRowNumbers= coerce $ gSourceIndexes g
  , gtGroupRowNumbers   = Comp $ V.enumFromN 0 <$> sizes
  , gtGroupNumber       = coerce $ V.enumFromN (0 :: Int) $ length sizes
  , gtTable             = t
  }
  where sizes = V.length <$> gSourceIndexes g

fullTableGroup :: Table -> GroupedTable I
fullTableGroup t = GroupedTable
  { gtGroupSizes        = I $ count t
  , gtToGroups          = const $ pure . coerce
  , gtOriginalRowNumbers= rns
  , gtGroupRowNumbers   = rns
  , gtGroupNumber       = coerce @Int 0
  , gtTable             = t
  }
  where size = count t
        rns = coerce $ V.enumFromN @Int 0 size
{-# INLINABLE fullTableGroup #-}

-- * Helper

runHQueryDyn :: GroupedTable f -> PrelimCols f -> HQueryDyn f g -> H (Maybe Symbol, TableCol' (f :.: g))
runHQueryDyn gt prelim = mapM (flip runReaderT (gt, prelim)) . fromNamedTableReader . unHQueryDyn
{-# INLINABLE runHQueryDyn #-}

runHQuery :: GroupedTable f -> HQuery f g a -> (Maybe Symbol, H ((f :.: g) a))
runHQuery gt = second (flip runReaderT (gt,[])) . fromNamedTableReader. unHQuery
{-# INLINABLE runHQuery #-}

runDynForgetName :: GroupedTable f -> HQueryDyn f g -> H (TableCol' (f :.: g))
runDynForgetName gt = fmap snd . runHQueryDyn gt []
{-# INLINABLE runDynForgetName #-}

runForgetName :: GroupedTable f -> HQuery f g a -> H ((f :.: g) a)
runForgetName = fmap2 snd runHQuery
{-# INLINABLE runForgetName #-}

runFullForgetName :: HQuery I g a -> Table -> H ((:.:) I g a)
runFullForgetName e = flip runForgetName e . fullTableGroup
{-# INLINE runFullForgetName #-}

runDynFullForgetName :: HQueryDyn I g -> Table -> H (TableCol' (I :.: g))
runDynFullForgetName e = flip runDynForgetName e . fullTableGroup
{-# INLINE runDynFullForgetName #-}

-- * delete

deleteW :: (HasCallStack, ToTableAndBack t) => Where -> t -> H' t
deleteW = useToTableAndBack . filter . fmap not
{-# INLINABLE deleteW #-}

delete :: (HasCallStack, ToTableAndBack t, ToVector f) => f Symbol -> t -> H' t
delete s = useToTableAndBack $ chainToH $ tableNoLengthCheck . H.deleteKeys (toVector s) . flipTable
{-# INLINABLE delete #-}

-- * select

--
-- todo:
-- select[n]
-- select[m n]
-- select[order]
-- select[n;order]
-- select distinct

select :: (ToTableAndBack t, HasCallStack) => ExpDyns I -> t -> H' t
select = useToTableAndBack . \case
  [] -> toH
  es -> chainToH @Table $ \t -> tableAnonymous . fmap2 singleGroupTableCol
    =<< selectRunHQueryDyn es (fullTableGroup t)
{-# INLINABLE select #-}

selectW :: (ToTableAndBack t, HasCallStack) => ExpDyns I -> Where -> t -> H' t
selectW es = useToTableAndBack . toFiltered select es
{-# INLINE selectW #-}

selectBy :: (ToTableAndBack t, HasCallStack) => ExpDyns Vector -> ExpDyns I -> t -> H' t
selectBy es' bys = useToTableAndBack $ case es' of
  [] -> toH
  es -> chainToH $ \t -> select bys t <&> snd . getGroupsAndGrouping >>= \idxs ->
    let g = fromGrouping idxs t
    in groupedTableAnonymous idxs g =<< selectRunHQueryDyn es g
{-# INLINABLE selectBy #-}

selectByW :: (ToTableAndBack t, HasCallStack) => ExpDyns Vector -> ExpDyns I -> Where -> t -> H' t
selectByW es = fmap useToTableAndBack . toFiltered2 selectBy es
{-# INLINE selectByW #-}

selectRunHQueryDyn :: Foldable t => t (HQueryDyn f Vector) -> GroupedTable f -> H (PrelimCols f)
selectRunHQueryDyn es tg = foldrM (\ex prelim -> (:prelim) <$> runHQueryDyn tg prelim ex) [] es

-- * agg
--
-- does not (currently) support `prelimCol`, `prelimColD`

aggBy :: (ToTable t, HasCallStack) => AggDyns Vector -> ExpDyns I -> t -> KeyedTableH
aggBy aggs bys =chainToH $ \t -> select bys t <&> getGroupsAndGrouping >>= \(keys, idxs) -> dict keys
  <=< tableAnonymous . fmap2 aggregatedTableCol <=< forM aggs $ runHQueryDyn (fromGrouping idxs t) []
{-# INLINABLE aggBy #-}

aggByW :: (ToTable t, HasCallStack) => AggDyns Vector -> ExpDyns I -> Where -> t -> KeyedTableH
aggByW = toFiltered2 aggBy
{-# INLINE aggByW #-}

agg :: (ToTable t, HasCallStack) => AggDyns I -> t -> TableRowH
agg aggs = chainToH $ \t -> tableRowAnonymous <=< forM aggs $ fmap3 tableCell $ runHQueryDyn (fullTableGroup t) []
{-# INLINABLE agg #-}


aggW :: (ToTable t, HasCallStack) => AggDyns I -> Where -> t -> TableRowH
aggW = toFiltered agg
{-# INLINE aggW #-}

-- * update

update :: (ToTableAndBack t, HasCallStack) => ExpDyns I -> t -> H' t
update e = useToTableAndBack $ \t -> mergeTablesPreferSecond t =<< select e t
{-# INLINABLE update #-}

-- | new columns are converted to Maybe (if they are not already) to be able to represent missing value
-- that could appear (or not, but only known at runtime depending on column contents)
updateW :: (ToTable t, HasCallStack) => ExpDyns I -> Where -> t -> TableH
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


updateBy :: ToTableAndBack t => ExpDyns V.Vector -> ExpDyns I -> t -> H' t
updateBy e by = useToTableAndBack $ \t -> mergeTablesPreferSecond t =<< selectBy e by t

-- | not sure what the semantics of this should be. should the where condition determine what rows are
-- updated or what rows enter the aggregation or both
--
-- updateByW = P.undefined

-- ** exec

execD :: (ToTable t, HasCallStack) => ExpDyn I -> t -> TableColH
execD e = chainToH $ fmap singleGroupTableCol . runDynFullForgetName e
{-# INLINABLE execD #-}

execDW :: (ToTable t, HasCallStack) => ExpDyn I -> Where -> t -> TableColH
execDW = toFiltered execD
{-# INLINE execDW #-}

exec :: forall a t . (ToTable t, HasCallStack) => Exp I a -> t -> VectorH a
exec e = chainToH $ coerce . runFullForgetName e
{-# INLINABLE exec #-}

execW :: forall a t . (ToTable t, HasCallStack) => Exp I a -> Where -> t -> VectorH a
execW = toFiltered exec
{-# INLINE execW #-}

execRec :: forall r t . (ToTable t, HasCallStack, TableRowT r) => Record (Exp I) r -> t -> TypedTableH r
execRec es = chainToH $ \t -> typedTable =<< R.mapM (coerce . flip runFullForgetName t) es
{-# INLINABLE execRec #-}

execRecW :: forall r t . (ToTable t, HasCallStack, TableRowT r)
  => Record (Exp I) r -> Where -> t -> TypedTableH r
execRecW = toFiltered execRec
{-# INLINABLE execRecW #-}

execRecBy :: forall r t . (ToTable t, HasCallStack, TableRowT r)
  => Record (Exp Vector) r -> ExpDyns I -> t -> TypedTableH r
execRecBy es bys = chainToH $ \t -> select bys t <&> snd . getGroupsAndGrouping >>= \idxs ->
  typedTable =<< R.mapM (fmap (redistributeGroups idxs) . runForgetName (fromGrouping idxs t)) es
{-# INLINABLE execRecBy #-}

execRecByW :: forall r t . (ToTable t, HasCallStack, TableRowT r)
  => Record (Exp Vector) r -> ExpDyns I -> Where -> t -> TypedTableH r
execRecByW = toFiltered2 execRecBy
{-# INLINABLE execRecByW #-}


execBy :: forall a t . (ToTable t, HasCallStack) => Exp Vector a -> ExpDyns I -> t -> VectorH a
execBy e bys = chainToH $ \t -> select bys t <&> snd . getGroupsAndGrouping >>= \idxs ->
  fmap (redistributeGroups idxs) $ runForgetName (fromGrouping idxs t) e
{-# INLINABLE execBy #-}

execByW :: forall a t . (ToTable t, HasCallStack) => Exp Vector a -> ExpDyns I -> Where -> t -> VectorH a
execByW = toFiltered2 execBy
{-# INLINE execByW #-}

-- | map to row
execRecAggBy :: forall g r t . (ToTable t, HasCallStack, TableRowT r, Hashable g)
        => Record (Agg Vector) r -> Exp I g -> t -> VectorDictH g (Record I r)
execRecAggBy aggs by = chainToH $ \t -> exec by t <&> getGroupsAndGrouping >>= \(keys, idxs) ->
  dict keys . flipTypedTable I =<< typedTable
  =<< R.mapM (coerce . runForgetName (fromGrouping idxs t)) aggs
{-# INLINABLE execRecAggBy #-}

execRecAggByW :: forall g r t . (ToTable t, HasCallStack, TableRowT r, Hashable g)
        => Record (Agg Vector) r -> Exp I g -> Where -> t -> VectorDictH g (Record I r)
execRecAggByW = toFiltered2 execRecAggBy
{-# INLINE execRecAggByW #-}

execRecAgg :: forall r t . (ToTable t, HasCallStack, TableRowT r) => Record (Agg I) r -> t -> H (Record I r)
execRecAgg aggs = chainToH $ \t -> R.mapM (coerce . flip runFullForgetName t) aggs
{-# INLINABLE execRecAgg #-}

execRecAggW :: forall r t . (ToTable t, HasCallStack, TableRowT r)
  => Record (Agg I) r -> Where -> t -> H (Record I r)
execRecAggW = toFiltered execRecAgg
{-# INLINABLE execRecAggW #-}

execMap :: forall v g t . (ToTable t, HasCallStack, HashableKey (Vector g))
  => Exp I v -> Exp I g -> t -> VectorDictH g v
execMap va ke t = chain2 dict (exec ke t) $ exec va t
{-# INLINABLE execMap #-}

execKey :: forall v g t . (ToKeyedTable t, HasCallStack, Hashable g)
  => Exp I v -> Exp I g -> t -> VectorDictH g v
execKey va ke = chainToH @KeyedTable $ \kt -> chain2 dict (exec ke $ key kt) $ exec va $ value kt
{-# INLINABLE execKey #-}

execAggBy :: forall v g t . (ToTable t, HasCallStack, Hashable g)
  => Agg Vector v -> Exp I g -> t -> VectorDictH g v
execAggBy agg by = chainToH $ \t -> (getGroupsAndGrouping <$> exec by t) >>= \(keys, idxs) ->
  dict keys . coerce =<< runForgetName (fromGrouping idxs t) agg
{-# INLINABLE execAggBy #-}

execAggByW :: forall v g t . (ToTable t, HasCallStack, Hashable g)
  => Agg Vector v -> Exp I g -> Where -> t -> VectorDictH g v
execAggByW = toFiltered2 execAggBy
{-# INLINABLE execAggByW #-}

execAgg :: forall a t . (ToTable t, HasCallStack) => Agg I a -> t -> H a
execAgg ag = chainToH $ coerce . runFullForgetName ag
{-# INLINABLE execAgg #-}

execAggW :: forall a t . (ToTable t, HasCallStack) => Agg I a -> Where -> t -> H a
execAggW = toFiltered execAgg
{-# INLINABLE execAggW #-}


-- * Where and filter

-- | this is always ungrouped. use `fby` or similar to filter based on grouped/aggregated values
type Where = Exp I Bool

runWhere :: ToTable t => Where -> t -> H WhereResult
runWhere wh = fmap fromMask . exec wh
{-# INLINABLE runWhere #-}

filter :: (HasCallStack, ToTableAndBack t) => Where -> t -> H' t
filter wh = useToTableAndBack $ chainToH $ \t -> applyWhereResult t =<< runWhere wh t
{-# INLINABLE filter #-}

filterBy :: (HasCallStack, ToTableAndBack t) => Exp Vector Bool -> ExpDyns I -> t -> H' t
filterBy wh bys = useToTableAndBack $ chainToH $ \t -> applyWhereResult t . fromMask =<< execBy wh bys t
{-# INLINABLE filterBy #-}

-- * Helper functions

toFiltered :: (HasCallStack, ToTable t) => (a -> Table -> H c) -> a -> Where -> t -> H c
toFiltered x a wh = chainToH @Table $ x a <=< filter wh 
{-# INLINE toFiltered #-}

toFiltered2 :: (HasCallStack, ToTable t) => (t1 -> t2 -> Table -> H c) -> t1 -> t2 -> Where -> t -> H c
toFiltered2 x a b wh = chainToH @Table $ x a b <=< filter wh 
{-# INLINE toFiltered2 #-}

toFiltered3 :: (HasCallStack, ToTable t) => (t1 -> t2 -> t3 -> Table -> H c) -> t1 -> t2 -> t3 -> Where -> t -> H c
toFiltered3 x a b c wh = chainToH @Table $ x a b c <=< filter wh 
{-# INLINE toFiltered3 #-}



-- * Sorting (unstable).
--
-- All functions check if the table is not already sorted

sortT :: forall e t . (HasCallStack, ToTable t) => Exp I e -> Comparison e -> t -> TableH
sortT w by = chainToH $ sortByM (exec w) by
{-# INLINABLE sortT #-}

ascT :: forall e t . (HasCallStack, ToTable t, Eq e, Ord e) => Exp I e -> t -> TableH
ascT w = sortT w compare
{-# INLINE ascT #-}

descT :: forall e t . (HasCallStack, ToTable t, Eq e, Ord e) => Exp I e -> t -> TableH
descT w = sortT w $ flip compare
{-# INLINE descT #-}

sortTD :: (HasCallStack, ToTable t) => ExpDyn I ->
  (forall f a . (Ord1 f, Ord a) => f a -> f a -> Ordering) -> t -> TableH
sortTD w by = chainToH $ \t -> withWrapped (\v -> sortBy (const $ co v) by t)
  <$> runDynFullForgetName w t
  where co = coerce :: (I :.: Vector) a -> Vector a
{-# INLINABLE sortTD #-}

ascTD :: (HasCallStack, ToTable t) => ExpDyn I -> t -> TableH
ascTD w = sortTD w compare1
{-# INLINABLE ascTD #-}

descTD :: (HasCallStack, ToTable t) => ExpDyn I -> t -> TableH
descTD w = sortTD w $ flip compare1
{-# INLINABLE descTD #-}

ascC :: TableCol -> TableCol
ascC = mapWrapped (sortBy id compare1)

descC :: TableCol -> TableCol
descC = mapWrapped (sortBy id $ flip compare1)

distinctByT :: (Hashable a, ToTable t) => Exp I a -> t -> TableH
distinctByT by = chainToH $ distinctByM $ exec by

distinctByTD :: ToH Table t => ExpDyn I -> t -> H Table
distinctByTD by = chainToH $ \t -> withWrapped (\v -> distinctBy (const $ co v) t)
  <$> runDynFullForgetName by t
  where co = coerce :: (I :.: Vector) (g a) -> Vector (One g a)

module Hoff.HQuery.Execution where

import           Control.Lens hiding (Empty, (<.))
import           Control.Monad.Reader
import           Data.Coerce
import           Data.SOP
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.HQuery.Expressions
import           Hoff.Table as H
import           Hoff.Utils
import           Hoff.Vector
import           Type.Reflection as R
import           Yahp hiding (filter, (:.:))

-- * Helper

fromGrouping :: Grouping -> Table -> GroupedTable Vector
fromGrouping i = GroupedTable (V.length <$> i) $ applyGrouping i

fullTableGroup :: Table -> GroupedTable I
fullTableGroup t = GroupedTable (I $ count t) I t
{-# INLINABLE fullTableGroup #-}

runHQueryDyn :: GroupedTable f -> HQueryDyn f g -> H (Maybe Symbol, TableCol' g)
runHQueryDyn gt = mapM (flip runReaderT gt) . fromNamedTableReader . unHQueryDyn
{-# INLINABLE runHQueryDyn #-}

runHQuery :: GroupedTable f -> HQuery f g a -> (Maybe Symbol, H (g a))
runHQuery gt = second (flip runReaderT gt) . fromNamedTableReader. unHQuery
{-# INLINABLE runHQuery #-}

runDynForgetName :: GroupedTable f -> HQueryDyn f g -> H (TableCol' g)
runDynForgetName = fmap3 snd runHQueryDyn
{-# INLINABLE runDynForgetName #-}

runForgetName :: GroupedTable f -> HQuery f g a -> H (g a)
runForgetName = fmap2 snd runHQuery
{-# INLINABLE runForgetName #-}
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
  es -> chainToH $ \t -> tableAnonymous <=< forM es $ fmap2 singleGroupTableCol
           . runHQueryDyn (fullTableGroup t)
{-# INLINABLE select #-}

selectW :: (ToTableAndBack t, HasCallStack) => ExpDyns I -> Where -> t -> H' t
selectW es = useToTableAndBack . toFiltered select es
{-# INLINE selectW #-}

selectBy :: (ToTableAndBack t, HasCallStack) => ExpDyns Vector -> ExpDyns I -> t -> H' t
selectBy es' bys = useToTableAndBack $ case es' of
  [] -> toH
  es -> chainToH $ \t -> (\idxs -> groupedTableAnonymous idxs <=< forM es $ runHQueryDyn
                         $ fromGrouping idxs t) . snd . getGroupsAndGrouping =<< select bys t
{-# INLINABLE selectBy #-}

selectByW :: (ToTableAndBack t, HasCallStack) => ExpDyns Vector -> ExpDyns I -> Where -> t -> H' t
selectByW es = fmap useToTableAndBack . toFiltered2 selectBy es
{-# INLINE selectByW #-}

-- * agg

aggBy :: (ToTable t, HasCallStack) => AggDyns Vector -> ExpDyns I -> t -> KeyedTableH
aggBy aggs bys = chainToH $ \t -> fmap (uncurry dictNoLengthCheck) . traverse
  (\idxs -> tableAnonymous <=< forM aggs $ runHQueryDyn $ fromGrouping idxs t)
  . getGroupsAndGrouping =<< select bys t
{-# INLINABLE aggBy #-}

aggByW :: (ToTable t, HasCallStack) => AggDyns Vector -> ExpDyns I -> Where -> t -> KeyedTableH
aggByW = toFiltered2 aggBy
{-# INLINE aggByW #-}

agg :: (ToTable t, HasCallStack) => AggDyns I -> t -> TableRowH
agg aggs = chainToH $ \t -> tableRowAnonymous <=< forM aggs $ runHQueryDyn $ fullTableGroup t
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



-- updateBy :: (ToTableAndBack t, HasCallStack) => ExpDyns Vector -> ExpDyns I -> t -> H' t
-- updateBy aggs bys = chainToH $ \t -> fmap (uncurry dictNoLengthCheck) . traverse
--   (\idxs -> tableAnonymous <=< forM aggs $ runHQueryDyn $ fromGrouping idxs t)
--   . getGroupsAndGrouping =<< select bys t


--   useToTableAndBack $ \t -> selectByRaw' aggs bys t >>= \(idxs, (_, aggTable)) ->
--   mergeTablesPreferSecond t (broadcastGroupValue idxs aggTable)
-- {-# INLINABLE updateBy #-}

-- -- ** exec

-- execD :: (ToTable t, HasCallStack) => ExpDyn -> t -> TableColH
-- execD = chainToH . runDynForgetName
-- {-# INLINE execD #-}

-- execDW :: (ToTable t, HasCallStack) => ExpDyn -> Where -> t -> TableColH
-- execDW = toFiltered execD
-- {-# INLINE execDW #-}

-- exec :: forall a t . (ToTable t, HasCallStack) => Exp a -> t -> VectorH a
-- exec = chainToH . runForgetName
-- {-# INLINE exec #-}

-- execW :: forall a t . (ToTable t, HasCallStack) => Exp a -> Where -> t -> VectorH a
-- execW = toFiltered exec
-- {-# INLINE execW #-}


-- execC :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Exp r -> t -> TypedTableH r
-- execC es = chainToH $ \t -> UnsafeTypedTable <$> R.mapM (\e -> runForgetName e t) es
-- {-# INLINABLE execC #-}

-- execCW :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Exp r -> Where -> t -> TypedTableH r
-- execCW = toFiltered execC
-- {-# INLINE execCW #-}

-- -- | map to row
-- execRBy :: forall g r t . (ToTable t, HasCallStack, TableRow r, Wrappable g)
--         => Record Agg r -> Exp g -> t -> VectorDictH g (Record I r)
-- execRBy aggs by = chainToH $ \t -> (getGroupsAndGrouping <$> exec by t) >>= \(keys, idxs) ->
--   dictNoLengthCheck keys . flipTypedTable I . UnsafeTypedTable
--   <$> R.mapM (flip runForgetName (fromGrouping idxs t)) aggs
-- {-# INLINABLE execRBy #-}

-- execRByW :: forall g r t . (ToTable t, HasCallStack, TableRow r, Wrappable g)
--         => Record Agg r -> Exp g -> Where -> t -> VectorDictH g (Record I r)
-- execRByW = toFiltered2 execRBy
-- {-# INLINE execRByW #-}

-- execRAgg :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Agg r -> t -> H (Record I r)
-- execRAgg aggs = chainToH $ \t -> R.mapM (fmap (I . V.head) . flip runForgetName (fullGroup t)) aggs
-- {-# INLINABLE execRAgg #-}

-- execRAggW :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Agg r -> Where -> t -> H (Record I r)
-- execRAggW = toFiltered execRAgg
-- {-# INLINABLE execRAggW #-}

-- -- | map
-- execMap :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Exp v -> Exp g -> t -> VectorDictH g v
-- execMap va ke t = dictNoLengthCheck <$> exec ke t <*> exec va t
-- {-# INLINABLE execMap #-}

-- execKey :: forall v g t . (ToKeyedTable t, HasCallStack, Wrappable g, Wrappable v) => Exp v -> Exp g -> t -> VectorDictH g v
-- execKey va ke = chainToH @KeyedTable $ \kt -> dictNoLengthCheck <$> exec ke (key kt) <*> exec va (value kt)
-- {-# INLINABLE execKey #-}

-- execBy :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Agg v -> Exp g -> t -> VectorDictH g v
-- execBy agg by t = do
--   bys <- exec by t
--   (_, (key, val)) <- selectByRaw (I $ ai agg) bys t
--   dictNoLengthCheck key <$> fromWrappedDyn Nothing (snd $ unI val)
-- {-# INLINABLE execBy #-}

-- execByW :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Agg v -> Exp g -> Where -> t -> VectorDictH g v
-- execByW = toFiltered2 execBy
-- {-# INLINE execByW #-}

-- execAgg :: forall a t . (ToTable t, HasCallStack) => Agg a -> t -> H a
-- execAgg ag = chainToH $ fmap V.head . runForgetName ag . fullGroup
-- {-# INLINABLE execAgg #-}

-- execAggW :: forall a t . (ToTable t, HasCallStack) => Agg a -> Where -> t -> H a
-- execAggW = toFiltered execAgg
-- {-# INLINABLE execAggW #-}


-- -- -- fmapWithErrorContext :: (a -> b) -> Exp a -> Exp b
-- -- -- fmapWithErrorContext f = V.imap (\i a -> f a

-- -- -- fromJust :: Exp (Maybe a) -> Exp a
-- -- -- fromJust me = ReaderT $ \t -> f t $ runReaderT me t
-- -- --   where f table mE = let res = V.catMaybes mE
-- -- --           in if length res == length mE then res
-- -- --           else do throwH $ UnexpectedNulls $ "\n" <> show (unsafeBackpermute (IterIndex <$> V.findIndices isNothing mE) table)
  
-- * Where and filter

-- | this is always ungrouped. use `fby` or similar to filter based on grouped/aggregated values
type Where = Exp I Bool

runWhere :: ToTable t => Where -> t -> H WhereResult
runWhere wh = chainToH @Table $ \t -> (coerce fromMask :: (I :.: Vector) Bool -> WhereResult)
                                      <$> runForgetName (fullTableGroup t) wh
{-# INLINABLE runWhere #-}

filter :: (HasCallStack, ToTableAndBack t) => Where -> t -> H' t
filter wh = useToTableAndBack $ \t -> chain2 applyWhereResult t $ runWhere wh t
{-# INLINABLE filter #-}

-- -- * Helper function

toFiltered :: (HasCallStack, ToTable t) => (a -> Table -> H c) -> a -> Where -> t -> H c
toFiltered x a wh = chainToH @Table $ x a <=< filter wh 
{-# INLINE toFiltered #-}

toFiltered2 :: (HasCallStack, ToTable t) => (t1 -> t2 -> Table -> H c) -> t1 -> t2 -> Where -> t -> H c
toFiltered2 x a b wh = chainToH @Table $ x a b <=< filter wh 
{-# INLINE toFiltered2 #-}

toFiltered3 :: (HasCallStack, ToTable t) => (t1 -> t2 -> t3 -> Table -> H c) -> t1 -> t2 -> t3 -> Where -> t -> H c
toFiltered3 x a b c wh = chainToH @Table $ x a b c <=< filter wh 
{-# INLINE toFiltered3 #-}



-- -- * Sorting (unstable)

-- sortT :: forall e t . (HasCallStack, ToTable t) => Exp e -> Comparison e -> t -> TableH
-- sortT w by = chainToH $ sortByWithM (runForgetName w) by
-- {-# INLINABLE sortT #-}

-- ascT :: forall e t . (HasCallStack, ToTable t, Eq e, Ord e) => Exp e -> t -> TableH
-- ascT w = sortT w compare
-- {-# INLINE ascT #-}

-- descT :: forall e t . (HasCallStack, ToTable t, Eq e, Ord e) => Exp e -> t -> TableH
-- descT w = sortT w $ flip compare
-- {-# INLINE descT #-}

-- sortTD :: (HasCallStack, ToTable t) => ExpDyn -> (forall f a . (Ord1 f, Ord a) => f a -> f a -> Ordering) -> t -> TableH
-- sortTD w by = chainToH $ \t -> withWrapped (\v -> sortByWith (const v) by t) <$> runDynForgetName w t
-- {-# INLINABLE sortTD #-}

-- ascTD :: (HasCallStack, ToTable t) => ExpDyn -> t -> TableH
-- ascTD w = sortTD w compare1
-- {-# INLINABLE ascTD #-}

-- descTD :: (HasCallStack, ToTable t) => ExpDyn -> t -> TableH
-- descTD w = sortTD w $ flip compare1
-- {-# INLINABLE descTD #-}

-- ascC :: TableCol -> TableCol
-- ascC = mapWrapped (sortByWith id compare1)

-- descC :: TableCol -> TableCol
-- descC = mapWrapped (sortByWith id $ flip compare1)

{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE PatternSynonyms    #-}

module Hoff.TypedTable
  (module Hoff.TypedTable
  ,module Reexport) where

import           Data.Default
import           Data.Record.Anon as R
import           Data.Record.Anon as Reexport (KnownFields, pattern (:=), K(..)
                                              , AllFields, RowHasField, Row)
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Table.Operations
import           Hoff.Table.Types
import           Hoff.Utils
import           Yahp as P hiding (reduce, take, drop, null)


type TableRowT r = (AllFields r Typeable, KnownFields r, AllFields r (Compose Iterable Vector))
type WrappableTableRow r = (TableRowT r, AllFields r Wrappable3)
type WrappableTableRowIorM r = (TableRowT r, AllFields r (WrappableIorM Vector))

newtype TypedTable' v (r :: Row Type) = UnsafeTypedTable { fromUnsafeTypedTable :: Record v r }

deriving via Record v r instance (KnownFields r, AllFields r (Compose Show v)) => Show (TypedTable' v r) 
deriving instance (KnownFields r, AllFields r (Compose Eq v)) => Eq (TypedTable' v r) 
deriving instance (KnownFields r, AllFields r (Compose Eq v), AllFields r (Compose Ord v))
  => Ord (TypedTable' v r) 
deriving instance KnownFields r => Semigroup (TypedTable r)
deriving instance KnownFields r => Monoid (TypedTable r)
deriving instance KnownFields r => Default (TypedTable r)

type TypedTable = TypedTable' Vector

type TypedTableH r = H (TypedTable r)
type TypedTableH' v r = H (TypedTable' v r)
type KeyedTypedTable k v = (Record Vector k, Record Vector v)

-- The class ToHWithRow works in theory, but then when using the resulting functions they need explicit type annotations. This is different from similar wrappert for Table, etc, because Table is not polymorphic itself..
-- class ToHWithRow tt (t2 :: Row Type -> Type) where
--   toHwithRow :: HasCallStack => tt -> H (t2 (RowT tt))
--   type RowT tt :: Row Type

-- instance ToHWithRow (a r) a where
--   toHwithRow = pure
--   type RowT (a r) = r

-- instance ToHWithRow (H (a r)) a where
--   toHwithRow = id
--   type RowT (H (a r)) = r


instance KnownFields r => Semigroup (Record Vector r) where
  (<>) = R.ap . R.map (Fn . (<>))
  {-# INLINABLE (<>) #-}
  sconcat (x :| xs) = R.map builderToVector
    $ foldl' (R.ap . R.map (Fn . appendVector)) (R.map builderFromVector x) xs
          
instance KnownFields r => Monoid (Record Vector r) where
  mempty = R.pure mempty
  {-# INLINE mempty #-}
  mconcat = maybe mempty sconcat . nonEmpty 
  {-# INLINABLE mconcat #-}
  
instance KnownFields r => Default (Record Vector r) where
  def = mempty
  {-# INLINE def #-}

instance KnownFields r => Default (Record (Const ()) r) where
  def = R.pure $ Const ()
  {-# INLINE def #-}

columnRecord :: TypedTable' v r -> Record v r
columnRecord = fromUnsafeTypedTable
{-# INLINE columnRecord #-}

typedTable :: forall r v . (KnownFields r, AllFields r (Compose Iterable v))
  => Record v r -> TypedTableH' v r
typedTable r = case equalLength $ toVector $ snd <$> ls of
  Just (_, True) -> throwH $ TableDifferentColumnLenghts $ show ls
  _              -> pure $ UnsafeTypedTable r 
  where ls = R.toList $ R.cmap (Proxy @(Compose Iterable v)) (K . I . count) r
{-# INLINABLE typedTable #-}

col :: forall a r v c . (KnownSymbol c, RowHasField c r a) => Field c -> TypedTable' v r -> v a
col f = R.get f . columnRecord
{-# INLINE col #-}


-- |
-- Example: rowsR @('[ "a" ':= Double, "c" ':= Char]) t1
-- 
rowsR :: forall r t . (HasCallStack, TableRowT r, ToTable t) => t -> VectorH (Record I r)
rowsR = chainToH @Table $ fmap (flipTypedTable I) . columns
{-# INLINABLE rowsR #-}

instance TableRowT r => FromTable (TypedTable r) where
  fromTable = columnsF
  {-# INLINE fromTable #-} 

-- | mostly used as helper
fromTableCommons :: (ToTable t, HasCallStack, TableRowT r) =>
  (forall a . (Typeable a, HasCallStack) => Maybe Symbol -> TableCol -> H (Vector a))
  -> t -> TypedTableH r
fromTableCommons fromW = chainToH $ \t -> fmap UnsafeTypedTable $ R.cmapM (Proxy @Typeable)
  (\(K name) -> fromW (Just $ toS name) =<< f t name) $ R.reifyKnownFields Proxy
  where f t = (flipTable t !) . toS
{-# INLINABLE fromTableCommons #-}
  
instance (WrappableTableRow r) => ToH Table (TypedTableH r) where
  toH = chain toH
  {-# INLINE toH #-}

-- | using `toWrappedDynIorM`
fromRows :: forall row r . (HasCallStack, WrappableTableRowIorM r) => Record ((->) row) r -> Vector row
  -> TableH
fromRows r v = table . fmap (first toS) . R.toList $ R.cmap (Proxy @(WrappableIorM Vector))
               (K . toWrappedDynIorM . (<$> v)) r
{-# INLINABLE fromRows #-}

-- | using `toWrappedDyn`
fromRowsF :: forall row r . (HasCallStack, WrappableTableRow r) => Record ((->) row) r -> Vector row
  -> TableH
fromRowsF r v = table . fmap (first toS) . R.toList $ R.cmap (Proxy @Wrappable3)
                (K . toWrappedDyn . (<$> v)) r
{-# INLINABLE fromRowsF #-}

instance (WrappableTableRow r) => ToH Table (TypedTable r) where
  toH = fromColumnsF
  {-# INLINE toH #-}

-- | using fromWrappedDyn
columns :: forall r t . (HasCallStack, ToTable t, TableRowT r) => t -> H (TypedTable r)
columns = fromTableCommons fromWrappedDyn
{-# INLINABLE columns #-}

columnsF :: forall r t . (HasCallStack,  ToTable t, TableRowT r) => t -> H (TypedTable r)
columnsF = fromTableCommons fromWrappedDynF
{-# INLINABLE columnsF #-}

-- | using `toWrappedDyn`
fromColumnsF :: (WrappableTableRow r, HasCallStack) => TypedTable r -> TableH
fromColumnsF = table . fmap (first toS) . R.toList . R.cmap (Proxy @Wrappable3)
              (K . toWrappedDyn) . columnRecord
{-# INLINABLE fromColumnsF #-}


-- | using `toWrappedDynIorM`
fromColumns :: (WrappableTableRowIorM r, HasCallStack) => TypedTable r -> TableH
fromColumns = table . fmap (first toS) . R.toList . R.cmap (Proxy @(WrappableIorM Vector))
              (K . toWrappedDynIorM) . columnRecord
{-# INLINABLE fromColumns #-}

 
flipTypedTable :: (TableRowT r, HasCallStack) => (forall a . a -> b a)
  -> TypedTable r -> Vector (Record b r)
flipTypedTable pureB cs = V.generate (count cs) $ \idx -> R.map (\v -> pureB $ V.unsafeIndex v idx)
  $ columnRecord cs
{-# INLINABLE flipTypedTable #-}
 
flipRecord :: forall r . (TableRowT r, HasCallStack) => Vector (Record I r) -> TypedTable r
flipRecord = UnsafeTypedTable . R.distribute'
{-# INLINABLE flipRecord #-}


-- * zipping: use `exec $ liftV3 g #col1 #col2 #col3` instead

-- zipRecord :: (a -> b -> x) -> TypedTable '["a" := a, "b" := b] -> Vector x
-- zipRecord f r = V.zipWith f (R.get #a r) (R.get #b r)

-- zipRecord3 :: (a -> b -> c -> x) -> TypedTable '["a" := a, "b" := b, "c" := c] -> Vector x
-- zipRecord3 f r = V.zipWith3 f (R.get #a r) (R.get #b r) (R.get #c r)

-- zipRecord4 :: (a -> b -> c -> d -> x) -> TypedTable '["a" := a, "b" := b, "c" := c, "d" := d] -> Vector x
-- zipRecord4 f r = V.zipWith4 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r)

-- zipRecord5 :: (a -> b -> c -> d -> e -> x) -> TypedTable '["a" := a, "b" := b, "c" := c, "d" := d, "e" := e] -> Vector x
-- zipRecord5 f r = V.zipWith5 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r) (R.get #e r)

-- zipRecord6 :: (a -> b -> c -> d -> e -> f -> x) -> TypedTable '["a" := a, "b" := b, "c" := c, "d" := d, "e" := e, "f" := f] -> Vector x
-- zipRecord6 f r = V.zipWith6 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r) (R.get #e r) (R.get #f r)

-- zipTable :: (Typeable a, Typeable b) => (a -> b -> x) -> Table -> Vector x
-- zipTable f = zipRecord f . columns

-- zipTable3 :: (Typeable a, Typeable b, Typeable c) => (a -> b -> c -> x) -> Table -> Vector x
-- zipTable3 f = zipRecord3 f . columns

-- zipTable4 :: (Typeable a, Typeable b, Typeable c, Typeable d) => (a -> b -> c -> d -> x) -> Table -> Vector x
-- zipTable4 f = zipRecord4 f . columns

-- zipTable5 :: (Typeable a, Typeable b, Typeable c, Typeable d, Typeable e) => (a -> b -> c -> d -> e -> x) -> Table -> Vector x
-- zipTable5 f = zipRecord5 f . columns

-- zipTable6 f = zipRecord6 f . columns

instance (KnownFields r, AllFields r (Compose Iterable v)) => Iterable (TypedTable' v r) where
  null = fromMaybe True . reduce null
  {-# INLINE null #-}
  count = fromMaybe 0 . reduce count
  {-# INLINABLE count #-}
  take n = UnsafeTypedTable . R.cmap (Proxy @(Compose Iterable v)) (take n) . columnRecord
  {-# INLINABLE take #-}
  drop n = mapTt $ drop n
  {-# INLINABLE drop #-}
  unsafeBackpermute i = mapTt $ unsafeBackpermute i
  {-# INLINABLE unsafeBackpermute #-}

-- * Helpers to implement Iterable (TypedTable' v r)

reduce :: forall v a r . (KnownFields r, AllFields r (Compose Iterable v))
  => (forall x . Iterable (v x) => v x -> a) -> TypedTable' v r  -> Maybe a
reduce f = headMaybe . R.collapse . R.cmap (Proxy @(Compose Iterable v)) (K . f) . columnRecord
{-# INLINABLE reduce #-}

mapTt :: forall v r . (KnownFields r, AllFields r (Compose Iterable v))
 => (forall x . Iterable (v x) => v x -> v x) -> TypedTable' v r -> TypedTable' v r
mapTt f = UnsafeTypedTable . R.cmap (Proxy @(Compose Iterable v)) f . columnRecord
{-# INLINABLE mapTt #-}

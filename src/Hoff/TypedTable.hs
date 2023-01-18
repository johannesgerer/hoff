{-# LANGUAGE UndecidableInstances #-}
-- needed for ?callStack
{-# LANGUAGE ImplicitParams #-}
{-# LANGUAGE PatternSynonyms    #-}

module Hoff.TypedTable
  (module Hoff.TypedTable
  ,module Reexport) where

import           Control.Monad.Writer (tell, runWriter)
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
import qualified Prelude as Unsafe
import           Yahp as P hiding (reduce, take, drop, reverse)


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

typedTable :: forall r v . (HasCallStack, KnownFields r, AllFields r (Compose Iterable v))
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
rowsR = chainToH @Table $ fmap (flipTypedTable I) . toTypedTable
{-# INLINABLE rowsR #-}

instance TableRowT r => FromTable (TypedTable r) where
  fromTable = toTypedTableF
  {-# INLINE fromTable #-} 

-- | mostly used as helper
fromTableCommons :: forall t r . (ToTable t, HasCallStack, TableRowT r) =>
  (forall a . (Typeable a, HasCallStack) => Maybe Symbol -> TableCol -> H (Vector a))
  -> t -> TypedTableH r
fromTableCommons fromW = chainToH $ \t -> do
  let transform :: Typeable x => K String x -> Writer [String] (Vector x)
      transform (K name) = runHWith (\e -> Unsafe.error "this should never be reached" <$ tell [show e]) pure
        $ let ?callStack = freezeCallStack emptyCallStack
          in fromW (Just $ toS name) =<< flipTable t ! toS name
  let (tt, errs) = runWriter $ R.cmapM (Proxy @Typeable) transform $ R.reifyKnownFields Proxy
  if P.null errs then pure $ UnsafeTypedTable tt
    else throwH $ withFrozenCallStack $ TypeMismatch
    $ "(es) in typed table conversion:\n" <> concat errs <> "\n\n" <> toS (showMetaWithDimensions t)
  
-- $mapHException (appendMsg $ 
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
  toH = fromTypedTableF
  {-# INLINE toH #-}

-- | using fromWrappedDyn
toTypedTable :: forall r t . (HasCallStack, ToTable t, TableRowT r) => t -> H (TypedTable r)
toTypedTable = fromTableCommons fromWrappedDyn
{-# INLINABLE toTypedTable #-}

toTypedTableF :: forall r t . (HasCallStack,  ToTable t, TableRowT r) => t -> H (TypedTable r)
toTypedTableF = fromTableCommons fromWrappedDynF
{-# INLINABLE toTypedTableF #-}

-- | using `toWrappedDyn`
fromTypedTableF :: (WrappableTableRow r, HasCallStack) => TypedTable r -> TableH
fromTypedTableF = fromRecordF . columnRecord
{-# INLINABLE fromTypedTableF #-}

-- | using `toWrappedDyn`
fromRecordF :: (WrappableTableRow r, HasCallStack) => Record Vector r -> TableH
fromRecordF = table . fmap (first toS) . R.toList . R.cmap (Proxy @Wrappable3) (K . toWrappedDyn)
{-# INLINABLE fromRecordF #-}


-- | using `toWrappedDynIorM`
fromTypedTable :: (WrappableTableRowIorM r, HasCallStack) => TypedTable r -> TableH
fromTypedTable = fromRecord . columnRecord
{-# INLINABLE fromTypedTable #-}

-- | using `toWrappedDynIorM`
fromRecord :: (WrappableTableRowIorM r, HasCallStack) => Record Vector r -> TableH
fromRecord = table . fmap (first toS) . R.toList . R.cmap (Proxy @(WrappableIorM Vector))
              (K . toWrappedDynIorM)
{-# INLINABLE fromRecord #-}

 
flipTypedTable :: (TableRowT r, HasCallStack)
  => (forall a . a -> b a) -> TypedTable r -> Vector (Record b r)
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
  null = fromMaybe True . reduce H.null
  {-# INLINE null #-}
  count = fromMaybe 0 . reduce count
  {-# INLINE count #-}
  take n = UnsafeTypedTable . R.cmap (Proxy @(Compose Iterable v)) (take n) . columnRecord
  {-# INLINE take #-}
  drop n = mapTt $ drop n
  {-# INLINE drop #-}
  unsafeBackpermute i = mapTt $ unsafeBackpermute i
  {-# INLINE unsafeBackpermute #-}
  reverse = mapTt reverse
  {-# INLINE reverse #-}

-- * Helpers to implement Iterable (TypedTable' v r)

reduce :: forall v a r . (KnownFields r, AllFields r (Compose Iterable v))
  => (forall x . Iterable (v x) => v x -> a) -> TypedTable' v r  -> Maybe a
reduce f = headMaybe . R.collapse . R.cmap (Proxy @(Compose Iterable v)) (K . f) . columnRecord
{-# INLINABLE reduce #-}

mapTt :: forall v r . (KnownFields r, AllFields r (Compose Iterable v))
 => (forall x . Iterable (v x) => v x -> v x) -> TypedTable' v r -> TypedTable' v r
mapTt f = UnsafeTypedTable . R.cmap (Proxy @(Compose Iterable v)) f . columnRecord
{-# INLINABLE mapTt #-}

filterTypedTable :: (KnownFields r, AllFields r (Compose Iterable Vector))
  => (TypedTable r -> Vector Bool) -> TypedTable r -> H (TypedTable r)
filterTypedTable wh tt = applyWhereResult tt (fromMask $ wh tt)
{-# INLINABLE filterTypedTable #-}

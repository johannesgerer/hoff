{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE QuantifiedConstraints #-}

module Hoff.Table.Operations where

import           Control.Lens hiding (Empty)
import           Data.Coerce
import qualified Data.DList as D
import qualified Data.HashMap.Strict as HM
import qualified Data.List
import qualified Data.Map.Strict as M
import qualified Data.Maybe
import           Data.SOP
import           Data.String as S
import qualified Data.Text as T
import           Data.Tuple.Select
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Debug.Trace
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Show
import           Hoff.Table.Show
import           Hoff.Table.Types
import           Hoff.Utils
import           Hoff.Vector
import qualified Prelude
import qualified Prelude as Unsafe
import           TextShow as B hiding (fromString)
import           Type.Reflection as R
import           Yahp as P hiding (typeRep, null)

instance ToTableAndBack Table where
  type InH Table = Table
  unsafeBack _ = id
  {-# INLINE unsafeBack #-}

instance ToTableAndBack TableH where
  type InH TableH = Table
  unsafeBack _ = id
  {-# INLINE unsafeBack #-}

instance ToTableAndBack KeyedTableH where
  type InH KeyedTableH = KeyedTable
  unsafeBack kt t = bind kt $ flip xkey t . toList . cols . key
  {-# INLINABLE unsafeBack #-}

instance ToTableAndBack KeyedTable where
  type InH KeyedTable = KeyedTable
  unsafeBack kt = unsafeBack $ pure @H kt
  {-# INLINE unsafeBack #-}

useToTableAndBack :: (HasCallStack, ToTableAndBack t) => (TableH -> TableH) -> t -> H' t
useToTableAndBack f t' = let (t, back) = toTableAndBack t' in back $ f t
{-# INLINE useToTableAndBack #-}

instance ToH Table KeyedTable               where toH = unkey
instance ToH Table KeyedTableH              where toH = unkey

instance DistinctIterable Table where
  distinct t | count (cols t) == 1 = mapTableNoLengthCheck (mapTableCol distinctV) t
             | True                = unsafeBackpermute (coerce $ distinctVIndices $ toUnsafeKeyVector t) t
  {-# INLINABLE distinct #-}
             

-- instance Iterable (Table' f) => DictComponent (Table' f) where
--   type KeyItem (Table' f)= TableKey' f
--   type Nullable (Table' f) = (Table' f)

instance Iterable Table => DictComponent Table where
  type KeyItem Table= TableKey
  type Nullable Table = Table



  -- {-# INLINE (!) #-}
  -- (!)             :: HasCallStack => Table          -> Int  -> TableKey
  -- t ! idx = A.map (\col -> I $ col V.! idx) t 

  unsafeBackpermuteMaybe = mapMTableWithNameNoLengthCheck . unsafeBackpermuteMaybeTableCol
  {-# INLINE unsafeBackpermuteMaybe #-}


  toUnsafeKeyVector t = V.generate (count t) (UnsafeTableKey t . IterIndex)
  {-# INLINABLE toUnsafeKeyVector #-}

  toCompatibleKeyVector = fmap2 toUnsafeKeyVector
    <$> orderedColumnIntersectionTable False (\_ _ _ x -> x)
  {-# INLINABLE toCompatibleKeyVector #-}
          
orderedColumnIntersectionTable :: HasCallStack =>
  Bool -> (forall g a . Wrappable2 g a => Vector (g a) -> Vector (g a) -> TableCol -> TableCol -> TableCol)
  -> Table -> Table -> TableH
orderedColumnIntersectionTable t comb a = table2 <=<
  orderedColumnIntersection t mempty mempty replicateNothing replicateNothing comb (flipTable a) . flipTable

-- | returns a subset of columns of the second table with column order and types as the first
-- 
-- the resulting columns are combined with the columns of the first table using the given combination function
orderedColumnIntersection :: HasCallStack => Bool
  -> (forall a . f1 (I a))
  -- ^ empty column of type f1
  -> (forall a . f2 (I a))
  -- ^ empty column of type f2
  -> (forall a . f1 (I None) -> f1 (Maybe a))
  -- ^ convert a column of `None`s to `Nothing`s
  -> (forall a . f2 (I None) -> f2 (Maybe a))
  -- ^ convert a column of `None`s to `Nothing`s
  -> (forall g a . Wrappable2 g a =>
      f1 (g a) -> f2 (g a) -> WrappedDyn f1 -> WrappedDyn f2 -> WrappedDyn f3)
  -- ^ a function that combine two columns. the function gets both the typed and wrapped versions of the
  -- columns and can use either/both to create the resulting column
  -> TableDict' f1 -> TableDict' f2 -> H (TableDict' f3)
orderedColumnIntersection requireIdenticalColumnSet mempty1 mempty2 fromNone1 fromNone2 combine
  sourceTable targetTable
  | Prelude.null allErrs        = dict requiredCols
                                  =<< zipTableDict3M combine2 sourceTable subTargetVals
  | True                        = throwH $ IncompatibleTables $ Prelude.unlines $ allErrs
  where (subTargetVals, additionalCols) = first value $ intersectAndDiff requiredCols targetTable
        combine2 name w1@(WrappedDyn t1@(App con1 _) v1) w2@(WrappedDyn t2@(App con2 _) v2)
          | Just HRefl <- t1    `eqTypeRep` t2                     = pure $ combine v1 v2 w1 w2
          | Just HRefl <- t2    `eqTypeRep` typeRep @(I None)
          , Just HRefl <- con1  `eqTypeRep` typeRep @Maybe       = pure $ combine v1 (fromNone2 v2) w1 w2
          | Just HRefl <- t1    `eqTypeRep` typeRep @(I None)
          , Just HRefl <- con2  `eqTypeRep` typeRep @Maybe       = pure $ combine (fromNone1 v1) v2 w1 w2
          | Just HRefl <- t2    `eqTypeRep` typeRep @(I Void)
          , Just HRefl <- con1  `eqTypeRep` typeRep @I           = pure $ combine v1 mempty2 w1 w2
          | Just HRefl <- t1    `eqTypeRep` typeRep @(I Void)
          , Just HRefl <- con2  `eqTypeRep` typeRep @I           = pure $ combine mempty1 v2 w1 w2
          | True                                                = throwH $ TypeMismatch
            $ "Column " <> toS name <> ": " <> show t1 <> " /= " <> show t2
        requiredCols = key sourceTable
        allErrs = catMaybes
          [("Missing from target table: " <> show (missing requiredCols targetTable))
           <$ guard (count requiredCols /= count subTargetVals)
          ,("Missing from source table: " <> show (key additionalCols))
           <$ guard (requireIdenticalColumnSet && not (null additionalCols))
          ]
{-# INLINABLE orderedColumnIntersection #-}


-- * from/to WrappedDyns

-- | convenience: accepts (f a) instead of (f (I a))
toWrappedDynI :: (ICoerce f a, Wrappable a) => f a -> WrappedDyn f
toWrappedDynI = WrappedDyn typeRep . coerceI
{-# INLINABLE toWrappedDynI #-}

class Functor f => ColumnData' f where
  emptyCol              :: f a
  constantColumn        :: b -> f a -> f b
  coerceFromI           :: f (I a) -> f a
  concatenateCol        :: f a -> Vector a

toNothingColumn :: ColumnData' f => f a -> f (Maybe b)
toNothingColumn = constantColumn Nothing
{-# INLINE toNothingColumn #-}

instance ColumnData' Vector where
  emptyCol              = V.empty
  {-# INLINE emptyCol #-}
  constantColumn        = flip replicateSameLength
  {-# INLINE constantColumn #-}
  coerceFromI           = coerce
  {-# INLINE coerceFromI #-}
  concatenateCol        = id
  {-# INLINE concatenateCol #-}
  
instance ColumnData' (Vector :.: Vector) where
  emptyCol              = Comp V.empty
  {-# INLINE emptyCol #-}
  constantColumn      c = Comp . fmap (flip replicateSameLength c) . unComp
  {-# INLINE constantColumn #-}
  coerceFromI           = coerce
  {-# INLINE coerceFromI #-}
  concatenateCol        = V.concatMap id . unComp
  {-# INLINE concatenateCol #-}

instance ColumnData' (Vector :.: I) where
  emptyCol              = Comp V.empty
  {-# INLINE emptyCol #-}
  constantColumn      c = Comp . flip replicateSameLength (I c) . unComp
  {-# INLINE constantColumn #-}
  coerceFromI           = coerce
  {-# INLINE coerceFromI #-}
  concatenateCol        = coerce
  {-# INLINE concatenateCol #-}

instance ColumnData' (I :.: Vector) where
  emptyCol              = Comp $ I V.empty
  {-# INLINE emptyCol #-}
  constantColumn      c = Comp . fmap (flip replicateSameLength c) . unComp
  {-# INLINE constantColumn #-}
  coerceFromI           = coerce
  {-# INLINE coerceFromI #-}
  concatenateCol        = coerce
  {-# INLINE concatenateCol #-}

-- | order of the two args in chosen such that useage of this gives nice type application possibilities
type ColumnData a f = (ColumnDataD f, Typeable a)
type ColumnDataD f = (ColumnData' (f :.: Vector), Functor f)
type ColumnData2 f g = (ColumnData' (f :.: g), Functor f, Functor g)

-- | convenience: unwraps (I a) to a
fromWrappedDyn :: forall a g . (ColumnData' g, Typeable a, HasCallStack)
  => Maybe Symbol -> TableCol' g -> H (g a)
fromWrappedDyn _ (WrappedDyn tr@(App con t) v)
  | Just HRefl <- con  `eqTypeRep` typeRep @I
  , Just HRefl <- t    `eqTypeRep` typeRep @a                  = pure $ coerceFromI v
  | Just HRefl <- tr   `eqTypeRep` typeRep @(I Void)            = pure emptyCol
fromWrappedDyn n w = fromWrappedDynF n w
{-# INLINABLE fromWrappedDyn #-}

-- | the full blown version (which has an inverse: toWrappedDyn (up to I None ~ Maybe a equivalence))
fromWrappedDynF :: forall a g . (ColumnData' g, Typeable a, HasCallStack)
  => Maybe Symbol -> TableCol' g -> H (g a)
fromWrappedDynF _ (WrappedDyn tf v)
  | Just HRefl <- ta   `eqTypeRep` tf                          = pure $ v
  | Just HRefl <- tf   `eqTypeRep` typeRep @(I None)
  , App cona _ <- ta
  , Just HRefl <- cona `eqTypeRep` typeRep @Maybe               = pure $ toNothingColumn v
  | Just HRefl <- tf   `eqTypeRep` typeRep @(I Void)            
  , App cona _ <- ta
  , Just HRefl <- cona `eqTypeRep` typeRep @I                   = pure emptyCol
  where ta  = typeRep @a
fromWrappedDynF name (WrappedDyn t _ ) = throwH $ TypeMismatch $
  "Column " <> maybe "<unknown>" show name <> ": Cannot convert " <> show t <> " to " <> show (typeRep @a)
{-# INLINABLE fromWrappedDynF #-}

-- * operations

mergeTablesWith :: (HasCallStack, ToTable a, ToTable b) => (Symbol -> TableCol -> TableColH) -> (Symbol -> TableCol -> TableColH)
                -> (Symbol -> TableCol -> TableCol -> TableColH) -> a -> b -> TableH
mergeTablesWith a b f = liftToH2 $ \t1 t2 -> table2 =<< on (fillDictWithM a b f) flipTable t1 t2
{-# INLINABLE mergeTablesWith #-}

mergeTablesPreferSecond :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
mergeTablesPreferSecond = mergeTablesWith untouchedA untouchedA keepSecondA
{-# INLINABLE mergeTablesPreferSecond #-}

mergeTablesPreferFirst :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
mergeTablesPreferFirst = mergeTablesWith untouchedA untouchedA keepFirstA
{-# INLINABLE mergeTablesPreferFirst #-}


(//) :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
(//) = mergeTablesPreferSecond
{-# INLINE (//) #-}

(\\) :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
(\\) = mergeTablesPreferFirst
{-# INLINE (\\) #-}

(/|) :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
(/|) = liftToH2 $ fmap2 table2 $ on (<>) flipTable
{-# INLINE (/|) #-}


infixr 2 //
infixr 2 /|
infixr 2 \\

ensureCol :: (ToTable t, Wrappable a, Wrappable1 f) => Symbol -> f a -> t -> TableH
ensureCol n v = chainToH @Table $ \t -> t \\ tcF n (replicateSameLength t v)
{-# INLINABLE ensureCol #-}


rename2 :: ToTable t => [(Symbol,Symbol)] -> t -> TableH
rename2 oldNewPairs = mapToH $ unsafeTableWithColumns_ %~ mapDictKeys (\on -> fromMaybe on $ HM.lookup on map)
  where map = HM.fromList oldNewPairs
{-# INLINABLE rename2 #-}

rename :: ToTable t => [Symbol] -> [Symbol] -> t -> TableH
rename oldNames newNames = rename2 $ zip oldNames newNames
{-# INLINABLE rename #-}


-- -- | first steps towards a grouped table (this is experimental)
-- and was done before the GroupedTable approach to HQuery. maybe it can be done by now...
--
-- xgroup :: ToTable t => Symbols -> t -> H (Table, GroupedTableRaw)
-- xgroup s = chainToH $ \table -> do
--   let (keyInput, valueInput)  = intersectAndDiff (toVector s) $ flipTable table
--   (keys, idxs) <- getGroupsAndGrouping <$> tableNoLengthCheck keyInput
--   (keys,) . mapTableWrapped (Comp . unsafeApplyGrouping idxs) <$> tableNoLengthCheck valueInput
-- {-# INLINABLE xgroup #-}


key' :: ToKeyedTable t => t -> H Table
key' = mapToH @KeyedTable key
{-# INLINE key' #-}

value' :: ToKeyedTable t => t -> H Table
value' = mapToH @KeyedTable value
{-# INLINE value' #-}

xkey :: (ToTable t, HasCallStack) => [Symbol] -> t -> KeyedTableH
xkey [] = \_ -> throwH $ TableWithNoColumn "xkey needs at least one key column"
xkey s = chainToH $ \t -> let (k,v) = intersectAndDiff (toVector s) $ flipTable t
                       in dictNoLengthCheck <$> tableNoLengthCheck k <*> tableNoLengthCheck v
{-# INLINABLE xkey #-}

-- | rename columns (using `rename2`) and then key by them
xkeyRename :: ToTable a => [(Symbol, Symbol)] -> a -> KeyedTableH
xkeyRename cols = xkey (snd <$> cols) . rename2 cols
{-# INLINABLE xkeyRename #-}

unkey :: ToKeyedTable kt => kt -> TableH
unkey = mapToH unkey'
{-# INLINABLE unkey #-}

unkey' :: KeyedTable -> Table
unkey' kt = unsafeTableWithRowIndices $ on (<>) flipTable (key kt) $ value kt

unkeyI :: Wrappable a => Symbol -> SingleKeyTable a -> Table
unkeyI keyName kt = unsafeTableWithRowIndices $ on (<>) flipTable (tc' keyName $ key kt) $ value kt
{-# INLINABLE unkeyI #-}

unkeyF :: (HasCallStack, Wrappable1 f, Wrappable a) => Symbol -> SingleKeyTable (f a) -> Table
unkeyF keyName kt =  unsafeTableWithRowIndices $ on (<>) flipTable (tcF' keyName $ key kt) $ value kt
{-# INLINABLE unkeyF #-}

count' :: (HasCallStack, ToTable t) => t -> H Int
count' = mapToH @Table count
{-# INLINE count' #-}

null' :: ToTable t => t -> H Bool
null' = mapToH @Table null
{-# INLINE null' #-}

take' :: ToTable t => Int -> t -> H Table
take' = mapToH . H.take
{-# INLINE take' #-}

drop' :: ToTable t => Int -> t -> H Table
drop' = mapToH . H.drop
{-# INLINE drop' #-}

distinct' :: ToTable t => t -> H Table
distinct' = mapToH distinct
{-# INLINE distinct' #-}

withMeta :: ToH Table b => b -> (TableH, b)
withMeta x = (meta x, x)
{-# INLINABLE withMeta #-}

meta :: (ToTable t, HasCallStack) => t -> TableH
meta t' = do t <- toTable t'
             table [("c", toWrappedDynI $ cols t), ("t", toWrappedDynI $ withWrapped' (\tr _ -> show tr) <$> vals t)]
{-# INLINABLE meta #-}
  
traceMeta :: ToTable b => b -> b
traceMeta x = Debug.Trace.trace (toS $ showMetaWithDimensions x) x
{-# INLINABLE traceMeta #-}
{-# WARNING traceMeta "'traceMeta' remains in code" #-}

showMetaWithDimensions :: ToTable t => t -> Text
showMetaWithDimensions x = showt (meta x) <> "\n" <> dimensions x <> "\n"
{-# INLINABLE showMetaWithDimensions #-}


-- fillCol :: forall a . Wrappable a => a -> TableCol ->  TableCol
-- fillCol val x@(WrappedDyn t col)
--   | Just HRefl <- t `eqTypeRep` expected                = x
--   | App con v <- t
--   , Just HRefl <- v `eqTypeRep` expected
--   , Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = toWrappedDyn $ fromMaybe val <$> col
--   | True                                                = throwH $ TypeMismatch $ "Expected (Maybe) " <> show expected <> " got " <> show t
--   where expected = typeRep :: TypeRep a


class TableList l where
  toTableList :: HasCallStack => l -> [(Maybe Text, TableH)]

instance {-# OVERLAPS #-} ToTable2 t => TableList t where
  toTableList = pure . (Nothing,) . toH
instance {-# OVERLAPS #-} ToTable2 t => TableList (Maybe Text, t) where
  toTableList = pure . second toH
instance {-# OVERLAPS #-} ToTable2 t => TableList (Text, t) where
  toTableList = pure . (Just *** toH)

instance {-# OVERLAPS #-} (ToTable2 t) => TableList [t] where
  toTableList = fmap ((Nothing,) . toH)

instance {-# OVERLAPS #-} (ToTable2 t) => TableList [(Maybe Text, t)] where
  toTableList = fmap2 toH

instance {-# OVERLAPS #-} (ToTable2 t) => TableList [(Text, t)] where
  toTableList = fmap (Just *** toH)

instance {-# OVERLAPS #-} (ToTable2 t) => TableList (HM.HashMap Text t) where
  toTableList = toTableList . HM.toList

instance {-# OVERLAPS #-} (ToTable2 t) => TableList (Map Text t) where
  toTableList = toTableList . M.toList

catMaybes' :: Typeable a => Maybe Symbol -> TableCol -> VectorH a
{-# INLINABLE catMaybes' #-}
catMaybes' n = \case
  c@(WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> pure mempty
    | Just HRefl <- con   `eqTypeRep` typeRep @I        -> fromWrappedDyn n c
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> fromWrappedDyn n $ toWrappedDynI $ V.catMaybes v
  w             -> errorMaybeOrI n w

catMaybes_ :: Maybe Symbol -> TableCol -> TableColH
catMaybes_ n = \case
  c@(WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> throwH $ TypeMismatch "catMaybes not supported for I None"
    | Just HRefl <- con   `eqTypeRep` typeRep @I        -> pure c
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> pure $ toWrappedDynI $ V.catMaybes v
  w             -> errorMaybeOrI n w
{-# INLINABLE catMaybes_ #-}

isJustCol :: (ColumnData' f, HasCallStack) => Maybe Symbol -> TableCol' f  -> H (f Bool)
isJustCol n = \case
  (WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> pure $ constantColumn False v
    | Just HRefl <- con   `eqTypeRep` typeRep @I        -> pure $ constantColumn True v
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> pure $ isJust <$> v
  w             -> errorMaybeOrI n w
{-# INLINABLE isJustCol #-}

isRightCol :: (Functor f, HasCallStack) => TableCol' f -> H (f Bool)
isRightCol = \case
  (WrappedDyn (App (App con _) _) v)
    | Just HRefl <- con `eqTypeRep` R.typeRep @Either   -> pure $ isRight <$> v
  (WrappedDyn tr _) -> throwH $ TypeMismatch $ "Expected I Either got " <> show tr

fromRightColUnsafe :: HasCallStack => TableCol' (I :.: Vector) -> TableColH' (I :.: Vector)
fromRightColUnsafe = \case
  (WrappedDyn (App (App con _) _) v)
    | Just HRefl <- con `eqTypeRep` R.typeRep @Either   ->
        pure $ toWrappedDynI $ fromRight Unsafe.undefined <$> v
  (WrappedDyn tr _) -> throwH $ TypeMismatch $ "Expected I Either got " <> show tr

fromLeftColUnsafe :: forall l . (HasCallStack, Wrappable l) => Proxy l -> TableCol' (I :.: Vector) -> TableColH' (I :.: Vector)
fromLeftColUnsafe _ = \case
  (WrappedDyn (App (App con l) _) v)
    | Just HRefl <- con `eqTypeRep` R.typeRep @Either   
    , Just HRefl <- l `eqTypeRep` R.typeRep @l -> pure $ toWrappedDynI $ fromLeft Unsafe.undefined <$> v
  (WrappedDyn tr _)      -> throwH
    $ TypeMismatch $ "Expected 'Either " <> show (R.typeRep @l) <> "' got " <> show tr
{-# INLINABLE fromLeftColUnsafe #-}

fromJustCol :: (ColumnData' f, HasCallStack) => Maybe (f Bool -> Text) -> Maybe Symbol -> TableCol' f ->  TableColH' f
fromJustCol someNothingsMsgM colNameM col'@(WrappedDyn t@(App con _) col)
  | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = 
    if V.all isJust $ concatenateCol col then pure $ toWrappedDyn $ I . Data.Maybe.fromJust <$> col
    else err $ maybe (": there are " <> show (length $ V.filter isNothing $ concatenateCol col)
                       <> " missing values") (toS . ($ isNothing <$> col)) someNothingsMsgM
  | Just HRefl <- t `eqTypeRep` R.typeRep @(I None)      = if null $ concatenateCol col
      then pure $ toWrappedDyn @(I Void) emptyCol
    else err ": whole column is None"
  | Just HRefl <- con `eqTypeRep` R.typeRep @I      = pure $ col'
  where err msg = throwH $ UnexpectedNulls $ "in Column " <> (maybe "<unknow>" toS colNameM) <> msg
        err :: String -> H a
fromJustCol _     name       w            = errorMaybeOrI name w

fromJustCol' :: HasCallStack => Table -> Maybe Symbol -> TableCol ->  TableColH
fromJustCol' t n = fromJustCol (Just msg) n
  where msg col = (": Showing rows with missing values\n" <>) . showTrunc 100 $ applyWhereResult t $ fromMask col

errorMaybeOrI :: HasCallStack => Maybe Symbol -> WrappedDyn f -> H a
{-# INLINABLE errorMaybeOrI #-}
errorMaybeOrI name (WrappedDyn t _  ) = withFrozenCallStack $ throwH $ TypeMismatch $ "in Column "
  <> maybe "<unknown>" toS name <> ": Expected Maybe or I got " <> show t

-- | turns the given columns to I and the other columns to Maybe
-- throwHs TypeMismatch and UnexpectedNulls
fromJusts :: (ToTable t, HasCallStack, ToVector f) => f Symbol -> t -> TableH
{-# INLINABLE fromJusts #-}
fromJusts columns = chainToH g
  where g table
          | null errs = flip mapMTableWithNameNoLengthCheck table $ \col v ->
              if V.elem col cvec then fromJustCol' table (Just col) v else toMaybe' col v
          | True      = throwH $ KeyNotFound $ ": The following columns do not exist:\n"
            <> show errs <> "\nAvailable:\n" <> show (cols table)
          where errs = missing cvec $ flipTable table
        cvec = toVector columns

allToMaybe :: (ToTable t, HasCallStack) => t -> TableH
allToMaybe  = chainToH $ mapMTableWithNameNoLengthCheck toMaybe'
{-# INLINABLE allToMaybe #-}

-- | convert column to Maybe (if it is not already)
toMaybe' :: (ColumnData' f, HasCallStack) => Symbol -> TableCol' f ->  TableColH' f
toMaybe' = toMaybe . Just

-- | convert column to Maybe (if it is not already)
toMaybe :: (ColumnData' f, HasCallStack) => Maybe Symbol -> TableCol' f ->  TableColH' f
toMaybe = modifyMaybeCol Nothing

allFromJusts :: (ToTable t, HasCallStack) => t -> TableH
allFromJusts = chainToH $ \t -> mapMTableWithNameNoLengthCheck (\col v -> fromJustCol' t (Just col) v) t
{-# INLINABLE allFromJusts #-}

data TableColModifierWithDefault f = TableColModifierWithDefault
  (forall g . (g -> Bool) -> g -> f g -> f g)

modifyMaybeCol :: forall f . (ColumnData' f, HasCallStack) => Maybe (TableColModifierWithDefault f)
  -> Maybe Symbol -> TableCol' f -> TableColH' f
modifyMaybeCol fM _ col'@(WrappedDyn t@(App con _) col)
  | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = g Nothing isNothing col' col
  | Just HRefl <- t   `eqTypeRep` R.typeRep @(I None)   = g (I $ None ()) (const True) col' col
  | Just HRefl <- t   `eqTypeRep` R.typeRep @(I Void)
  = g (I $ None ()) (Unsafe.error "this should never happen908237. how could this function be called on an empty column???")
    (toWrappedDyn @(I None) emptyCol) emptyCol
  | Just HRefl <- con `eqTypeRep` R.typeRep @I          =
      let c = Just <$> coerceFromI col in g Nothing (const False) (toWrappedDyn c) c
  where g :: forall g a . Wrappable2 g a => g a -> (g a -> Bool) -> TableCol' f -> f (g a) -> TableColH' f
        g v isNoth wrapped raw = case fM of
          Just (TableColModifierWithDefault f)  -> pure $ toWrappedDyn $ f isNoth v raw
          _                                     -> pure wrapped
modifyMaybeCol  _ name       w            = errorMaybeOrI name w
{-# INLINABLE modifyMaybeCol #-}
                               
voidCol :: WrappedDyn Vector
voidCol = toWrappedDynI $ mempty @(Vector Void)

unsafeBackpermuteMaybeTableCol :: HasCallStack => Vector (Maybe IterIndex) -> Symbol -> TableCol
  -> TableColH
unsafeBackpermuteMaybeTableCol ks _ (WrappedDyn t@(App con _) col)
  | Just HRefl <- t     `eqTypeRep` R.typeRep @(I None)     = pure $ replicateNone ks
  | Just HRefl <- t     `eqTypeRep` R.typeRep @(I Void)     = pure $ replicateNone ks
  | Just HRefl <- con   `eqTypeRep` R.typeRep @I          =
      pure $ toWrappedDyn $ unsafeBackpermuteMaybeV (coerceUI col) (coerce ks)
  | Just HRefl <- con   `eqTypeRep` R.typeRep @Maybe      =
      pure $ toWrappedDyn $ fmap join $ unsafeBackpermuteMaybeV col (coerce ks)
unsafeBackpermuteMaybeTableCol _ name       w            = errorMaybeOrI (Just name) w

replicateNone :: Iterable a => a -> WrappedDyn Vector
replicateNone = toWrappedDyn . flip replicateSameLength (I $ None ())
{-# INLINABLE replicateNone #-}

-- * joins
-- missing: ^ Fill table (todo)

-- | helper for joins
combineCols :: HasCallStack => 
  (forall g a . Wrappable2 g a => Vector (g a) -> Vector (g a) -> TableCol) -> Symbol -> TableCol
  -> TableCol -> TableColH
combineCols combineVec name (WrappedDyn t1@(App con1 v1) col1) (WrappedDyn t2@(App con2 v2) col2) 
  | Just HRefl <- t1 `eqTypeRep` t2             = pure $ combineVec col1 col2
  | Just HRefl <- t1 `eqTypeRep` typeRep @(I Void) = pure $ combineVec mempty col2
  | Just HRefl <- t2 `eqTypeRep` typeRep @(I Void) = pure $ combineVec col1 mempty
  | Just HRefl <- con1 `eqTypeRep` typeRep @I
  , Just HRefl <- con2 `eqTypeRep` typeRep @Maybe = case () of
      () | Just HRefl <- v1 `eqTypeRep` typeRep @None -> pure $ combineVec (replicateNothing col1) col2
         | Just HRefl <- v1 `eqTypeRep` v2            -> pure $ combineVec (Just <$> coerceUI col1) col2
      _ -> err
  | Just HRefl <- con2 `eqTypeRep` typeRep @I
  , Just HRefl <- con1 `eqTypeRep` typeRep @Maybe = case () of
      () | Just HRefl <- v2 `eqTypeRep` typeRep @None -> pure $ combineVec col1 (replicateNothing col2)
         | Just HRefl <- v1 `eqTypeRep` v2            -> pure $ combineVec col1 (Just <$> coerceUI col2)
      _ -> err
  | True  = err
  where err = tableJoinError name t1 t2
{-# INLINABLE combineCols #-}


-- combineMaybe combineVec v1 v2 col1 col2
  -- | Just HRefl <- v1 `eqTypeRep` typeRep @None = pure $ combineVec (replicateNothing col1) col2

tableJoinError :: HasCallStack => (Show a2, Show a3) => Symbol -> a3 -> a2 -> H a
tableJoinError name t1 t2 = withFrozenCallStack $ throwH $ TypeMismatch
  $ "Column " <> toS name <> ": trying to join " <> show t2 <> " onto " <> show t1
{-# INLINABLE tableJoinError #-}


-- | inner join
-- joins ONE row of the second table to each row of the first table
--
-- check out ijc for more SQL-like join behavior
ij :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE ij #-}
ij = liftToH2 @Table @KeyedTable $ \t kt ->
  (V.unzip <$> safeMapAccessH (\hm -> V.imapMaybe (\i -> fmap (IterIndex i,) . (hm HM.!?))) t kt)
  >>= \(idxsLeft, idxs) -> mergeTablesPreferSecond (unsafeBackpermute idxsLeft t)
                           $ unsafeBackpermute idxs $ value kt

-- | if you do not care which columns are taken from what table, this one will swap the join order to
-- optimize performance
ijFaster :: (ToTable t, ToTable t2, HasCallStack) => [Symbol] -> t -> t2 -> TableH
ijFaster keys = liftToH2 @Table $ applySmallerFirst $ \t t2 -> ij t =<< xkey keys t2

-- | remove any matches, i.e. keep exactly what is dropped in `ij`
diff :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
diff = liftToH2 @Table @KeyedTable $ \t kt ->
  applyWhereResult t . fromMask =<< safeMapAccessH (fmap . fmap not . flip HM.member) t kt

-- | inner join (cartesian)
ijc :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE ijc #-}
ijc = liftToH2 $ \t (kt :: KeyedTable) -> do
  let groupsToIndices = unsafeGroupingDict kt 
  (idxsRowLeft :: Vector IterIndex, idxsGroupRight :: Vector GroupIndex)  <- V.unzip <$> safeMapAccessH
    (\hm -> V.imapMaybe (\i -> fmap (IterIndex i,) . (coerce1 @IterIndex @GroupIndex hm HM.!?)))
    t groupsToIndices
  let (idxsRight, idxsLeft) = groupingToConcatenatedIndicesAndDuplication (value groupsToIndices)
                              idxsGroupRight idxsRowLeft
  mergeTablesPreferSecond (unsafeBackpermute idxsLeft t) $ unsafeBackpermute idxsRight $ value kt

-- | if you do not care which columns are taken from what table, this one will swap the join order to
-- optimize performance
ijcFaster :: (ToTable t, ToTable t2, HasCallStack) => [Symbol] -> t -> t2 -> TableH
ijcFaster keys = liftToH2 @Table $ applySmallerFirst $ \t t2 -> ijc t =<< xkey keys t2

-- | takes a grouping (N groups, with group lenghts: n_i for i ≤ N) and returns two vectors of length Σ
-- n_i. The first is a concatenation of the group indices
groupingToConcatenatedIndicesAndDuplication :: RawGrouping
  -- ^ N groups, with group lenghts: n_i for i ≤ N
  -> Vector GroupIndex
  -- ^ vector of indices g_j for j ≤ M, with g_j ≤ N
  -> Vector a
  -- ^ vector with elements x_i for i ≤ M
  -> (Vector IterIndex
     -- ^ concatenation of source indices pointed to by the group indices g_j. Length Σ n_(g_j) for j ≤ N
     , Vector a
     -- ^ concatenation of repeated elements of the input vector (in input order). The value x_i is repeated n_(g_i) times.
     )
{-# INLINABLE groupingToConcatenatedIndicesAndDuplication #-}
groupingToConcatenatedIndicesAndDuplication grp grpIdxs vec =
  (V.concat $ toList permutedGroupIdxs
  , V.concat $ V.toList $ V.zipWith replicateSameLength permutedGroupIdxs vec)
  where permutedGroupIdxs = unsafeBackpermute (coerce1 @GroupIndex @IterIndex grpIdxs) grp


-- | left join
lj :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE lj #-}
lj = liftToH2 $ \t (kt :: KeyedTable) -> do
  idxs <- safeMapAccessH (fmap . (HM.!?)) t kt
  let fill  = combineCols $ \col1 col2 -> toWrappedDyn
        $ V.zipWith (\old new -> fromMaybe old new) col1
        $ unsafeBackpermuteMaybeV col2 $ coerce idxs
  if all isJust idxs then
    mergeTablesWith untouchedA toMaybe' (combineCols $ const toWrappedDyn)
    t $ unsafeBackpermute (Data.Maybe.fromJust <$> idxs) $ value kt
    else mergeTablesWith untouchedA (unsafeBackpermuteMaybeTableCol idxs) fill t $ value kt

-- | left join (cartesian)
ljc :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE ljc #-}
ljc = liftToH2 $ \t (kt :: KeyedTable) -> do
  let groupsToIndices = unsafeGroupingDict kt
  idxsGroupRight <- safeMapAccessH (fmap . (HM.!?)) t groupsToIndices
  let (idxsRight, idxsLeft) = groupingToConcatenatedIndicesAndDuplicationM (value groupsToIndices) idxsGroupRight $
                              coerce $ unsafeTableRowNumbers t
      inputPermute _ = pure . mapWrapped (unsafeBackpermute idxsLeft)
      fill  = combineCols $ \col1 col2 -> toWrappedDyn $ unsafeBackpermuteMaybe2 col1 col2 (coerce idxsLeft) $ coerce idxsRight
  if all isJust idxsGroupRight then
    mergeTablesWith inputPermute toMaybe' (combineCols $ const toWrappedDyn) t
    $ unsafeBackpermute (Data.Maybe.fromJust <$> idxsRight) $ value kt
    else mergeTablesWith inputPermute (unsafeBackpermuteMaybeTableCol idxsRight) fill t $ value kt


-- | see docs of `groupingToConcatenatedIndicesAndDuplication`
groupingToConcatenatedIndicesAndDuplicationM :: RawGrouping -> Vector (Maybe IterIndex) -> Vector a
  -> (Vector (Maybe IterIndex), Vector a)
groupingToConcatenatedIndicesAndDuplicationM grp grpIdxs vec =
  (V.concat $ toList $ maybe (V.singleton Nothing) (fmap Just) <$> permutedGroupIdxs
  , V.concat $ V.toList $ V.zipWith (maybe V.singleton replicateSameLength) permutedGroupIdxs vec)
  where permutedGroupIdxs = unsafeBackpermuteMaybeV grp $ coerce grpIdxs
{-# INLINABLE groupingToConcatenatedIndicesAndDuplicationM #-}


-- | prepend (first arg) and append (second arg) Nothing's to a column vector (turning it to Maybe as needed)
padColumn :: HasCallStack => Int -> Int -> Symbol -> TableCol -> TableColH
padColumn prependN appendN col = flip modifyMaybeCol (Just col)
  $ Just $ TableColModifierWithDefault $ \_ def -> 
  let  prepend | prependN <= 0 = id
               | True          = (V.replicate prependN def <>)
       append  | appendN <= 0  = id
               | True          = (<> V.replicate appendN def)
  in prepend . append

-- | union join table (converting columns to Maybe as needed and filling with nulls etc)
ujt :: (ToTable t, ToTable g, HasCallStack) => t -> g -> TableH
ujt = liftToH2 @Table @Table $ \t1 t2 -> mergeTablesWith (padColumn 0 $ count t2) (padColumn (count t1) 0)
  (combineCols $ \a b -> toWrappedDyn $ a <> b) t1 t2
{-# INLINABLE ujt #-}


-- | union join matching table (requires identical columns)
ujmt :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
ujmt = liftToH2 $ orderedColumnIntersectionTable True $ \v1 v2 _ _ -> toWrappedDyn $ v1 <> v2
{-# INLINABLE ujmt #-}

-- | union join matching tables (requires identical columns)
concatMatchingTables :: HasCallStack => NonEmpty Table -> TableH
concatMatchingTables (t :| ts) = table2 . mapDictValues
  (mapWrapped builderToVector) =<< foldM g t0 ts
  where t0 = mapDictValues (mapWrapped builderFromVector) $ flipTable  t :: TableDict' VectorBuilder
        g x = orderedColumnIntersection True
          (Comp D.empty)
          mempty
          (builderFromVector . flip V.replicate Nothing . sum . fmap V.length . unComp)
          replicateNothing
          (\b a _ _ -> toWrappedDyn $ appendVector b a) x . flipTable

replicateTable :: Int -> Table -> Table
replicateTable n = mapTableWrappedNoLengthCheck $ V.concat . Prelude.replicate n

-- | carefull! this is usually O((resulting rows)^2) because table concatenation is O(resulting rows)
fold1TableH :: ToTable t => (TableH -> TableH -> TableH) -> NonEmpty t -> TableH
fold1TableH f = foldl1' f . fmap toH
{-# INLINE fold1TableH #-}


-- | union join keyed tables
--
-- 
-- prefers the second, even if the value is `Nothing` (see Tests.hs)
ujk :: (ToKeyedTable t, ToKeyedTable g, HasCallStack) => t -> g -> KeyedTableH
ujk = liftToH2 @KeyedTable @KeyedTable $ \kt1 kt2 -> do
 combinedUniqueKeys <- distinct <$> on ujmt key kt1 kt2
 let (idxs1, idxs2) = fmap (unsafeMap kt1 HM.!?) &&& fmap (unsafeMap kt2 HM.!?) $ toUnsafeKeyVector
                      $ combinedUniqueKeys
     fill = combineCols $ \v1 v2 -> toWrappedDyn $ Hoff.Vector.unsafeBackpermuteMaybe2 v1 v2
                                    (coerce $ Data.Maybe.fromJust <$> idxs1) $ coerce idxs2
 dictNoLengthCheck combinedUniqueKeys <$> mergeTablesWith (unsafeBackpermuteMaybeTableCol idxs1)
   (unsafeBackpermuteMaybeTableCol idxs2) fill (value kt1) (value kt2)
{-# INLINABLE ujk #-}
        

-- -- | union join keyed table (cartesian)
-- ujc :: HasCallStack => KeyedTable -> KeyedTable -> KeyedTable

-- | cartesian product
-- the resulting row order is not defined
cross :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
cross = liftToH2 @Table @Table $ \t1 t2 -> uncurry mergeTablesPreferSecond
  $ bool (swap $ g (t2, t1)) (g (t1,t2)) (count t2 > count t1)
  where g (a, b) = (unsafeBackpermute idxs a, replicateTable (count a) b)
          where idxs = coerce1 @Int @IterIndex $ V.concatMap (V.replicate cb) $ unsafeTableRowNumbers a
                cb = count b
{-# INLINABLE cross #-}
        


-- * convert to statically typed, or assure tables adheres to static schema

keyed :: (HasCallStack, ToTable k, ToTable v) => k -> v -> KeyedTableH
keyed = liftToH2 dict
{-# INLINE keyed #-}

fromKeyed :: (HasCallStack, FromTable k, FromTable v, ToKeyedTable kt) => kt -> (H k, H v)
fromKeyed kt = (fromTable =<< key' kt, fromTable =<< value' kt)
{-# INLINABLE fromKeyed #-}

-- keyedTypedTableOld :: forall k v t . (HasCallStack, WrappableTableRow k, WrappableTableRow v, ToKeyedTable t)
--   => t -> H (KeyedTypedTableOld k v)
-- keyedTypedTableOld = fmap UnsafeKeyedTypedTableOld . uncurry keyed . fromKeyed @(TableR k) @(TableR v)
-- {-# INLINABLE keyedTypedTableOld #-}

-- typedTable :: forall r t . (HasCallStack, WrappableTableRow r, ToTable t) => t -> H (TypedTableOld r)
-- typedTable = fmap UnsafeTypedTableOld . toH . chain (fromTable @(TableR r)) . toH
-- {-# INLINABLE typedTable #-}

-- | no bounds checking
unsafeRowDict :: Table -> Int -> TableRowDict
unsafeRowDict t i = dictNoLengthCheck (cols t) $ withWrapped (toWrappedDyn . I . (flip V.unsafeIndex i)) <$> vals t

rowDicts :: ToH Table t => t -> VectorH TableRowDict
rowDicts = mapToH $ \t -> V.generate (count t) $ unsafeRowDict t
{-# INLINABLE rowDicts #-}

-- | if all columns are of the same type, one can get row vector dicts
rowVectors :: (Typeable a, HasCallStack, ToH Table t) => t -> VectorH (VectorDict Symbol a)
rowVectors = chainToH $ \t ->
  (\d -> V.generate (count @Table t) $ flip mapDictValues d . flip V.unsafeIndex) <$> fromDynTable t
{-# INLINABLE rowVectors #-}


-- | if all columns are of the same type, one can get a vector dict of columns
fromDynTable :: (Typeable a, HasCallStack, ToTable t) => t -> VectorDictH Symbol (Vector a)
fromDynTable = chainToH $ mapDictWithKeyM (fromWrappedDyn . Just) . flipTable
{-# INLINABLE fromDynTable #-}

dimensions :: ToH Table a => a -> Text
dimensions = runHWith shot
  (\t -> showt (count @Table t) <> " rows, " <> showt (count $ cols t) <> " cols") . toH 

metas :: TableList a => a -> Text
metas = foldMap (\(n,r) -> showt n <> "\n" <> showMetaWithDimensions r <> "\n") . toTableList

printMetas :: (MonadIO m, TableList a) => a -> m ()
printMetas = putStrLn . metas 
{-# INLINE printMetas #-}

summaries :: TableList a => a -> Text
summaries = foldMap (\(n,r) -> showt n <> "\n" <> showt (summary r) <> "\n") . toTableList

printSummaries :: (MonadIO m, TableList a) => a -> m ()
printSummaries = putStrLn . summaries
{-# INLINE printSummaries #-}

metaAndSummaries :: (TableList a) => a -> Text
metaAndSummaries = foldMap g . toTableList
  where g (n,r) = T.unlines [showt n,"\n", showt $ meta r, showt $ summary r, ""]

printMetaAndSummaries :: (MonadIO m, TableList a) => a -> m ()
printMetaAndSummaries = putStrLn . metaAndSummaries
{-# INLINE printMetaAndSummaries #-}

wrapErrorMeta :: ToH Table t => t -> H a -> H a
wrapErrorMeta = mapHException . appendMsg . toS . ("\n\n" <>) . showMetaWithDimensions


summaryKeys :: [Text]
summaryKeys = ["type"
                -- ,"count"
              ,"non nulls"
              ,"nulls"
              ,"unique"
              ,"min"
              ,"max"
              ,"most frequent"
              ,"frequency"]

summaryOld :: ToTable t => t -> KeyedTableH
summaryOld = chainToH $ \t -> dict (tc' (s t) $ summaryKeys)
                              =<< mapMTableWithNameNoLengthCheck summaryOldCol t
  where s t = "s: " <> dimensions t
{-# INLINABLE summary #-}

summaryOldCol :: HasCallStack => Symbol -> TableCol -> TableColH
summaryOldCol name = \case
  c@(WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> summaryOldVec c $ V.empty @()
    | Just HRefl <- con   `eqTypeRep` typeRep @I        -> summaryOldVec c $ coerceUI v
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> summaryOldVec c $ V.catMaybes v
  w             -> errorMaybeOrI (Just name) w

summary ::  (HasCallStack, ToTable t) => t -> KeyedTableH
summary = chainToH @Table $ \(toSnd flipTable -> (t, td)) -> do
  cols <- zipDictWithM calc td
  dict (tc' ("s: " <> dimensions t) $ key td) =<< table
     [("type"           , toWrappedDynI $ sel1 <$> cols)
     ,("non nulls"      , toWrappedDynI $ sel2 <$> cols)
     ,("nulls"          , toWrappedDynI $ sel3 <$> cols)
     ,("unique"         , toWrappedDyn $ sel4 <$> cols)
     ,("min"            , toWrappedDyn $ sel5 <$> cols)
     ,("max"            , toWrappedDyn $ sel6 <$> cols)
     ,("most frequent"  , toWrappedDyn $ sel7 <$> cols)
     ,("frequency"      , toWrappedDyn $ sel8 <$> cols)
     ]
  where calc :: Symbol -> TableCol -> H SummaryHelper
        calc name c@(WrappedDyn tr v) = case tr of
          (App con _)
            | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> summaryCalc tr v $ V.empty @()
            | Just HRefl <- con   `eqTypeRep` typeRep @I        -> summaryCalc tr v $ coerceUI v
            | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> summaryCalc tr v $ V.catMaybes v
          _             -> errorMaybeOrI (Just name) c
  
type SummaryHelper = (Text, Int, Int, Maybe Int, Maybe Text, Maybe Text, Maybe Text, Maybe Int)
summaryCalc :: forall m t b a . (Ord b, AtomShow b, Applicative m, Show t, Hashable b)
  => t -> Vector a -> Vector b -> m SummaryHelper
summaryCalc tr fullVec nonnull = pure (shot tr
                                      , nl
                                      , fl - nl
                                      , guardNonNull $ length $ distinct nonnull
                                      , mm V.minimum
                                      , mm V.maximum
                                      ,fst maxFreq
                                      ,snd maxFreq)
  where (fl, nl) = (length fullVec, length nonnull)
        mm f = toText $ f $ nonnull 
        guardNonNull = (<$ guard (not $ null nonnull)) 
        guardNonNull :: x -> Maybe x
        toText :: b -> Maybe Text
        toText =  guardNonNull . toS . toLazyText . buildAtomInColumn
        maxFreq = toText *** guardNonNull $ Data.List.maximumBy (comparing snd) $ HM.toList
                  $ HM.fromListWith (+) $ toList $ (,1::Int) <$> nonnull
        

summaryOldVec :: forall a . Wrappable a => TableCol -> Vector a -> TableColH
summaryOldVec (WrappedDyn tr fullVec) nonnull = pure $ toWrappedDynI $ toVector
  [shot tr
  -- ,showt fullVecLength
  ,showt $ length nonnull
  ,nulls
  ,showt $ length $ distinct nonnull
  ,mm V.minimum
  ,mm V.maximum
  ,fst maxFreq
  ,snd maxFreq]
  where mm f | null nonnull     = "n/a" :: Text
             | True             = toText $ f $ nonnull 
        nulls = showt $ fullVecLength - length nonnull
        maxFreq | null nonnull  = ("n/a", "0")
                | True          = toText *** showt $ Data.List.maximumBy (comparing snd) $ HM.toList
                                  $ HM.fromListWith (+) $ toList $ (,1::Int) <$> nonnull
        toText :: a -> Text
        toText = toS . toLazyText . buildAtomInColumn
        fullVecLength = length fullVec
{-# INLINABLE summaryOldVec #-}

-- | all columns are `Maybe`
--
-- empty table -> all columns `I None`, i.e. castable to any `Maybe x`
vectorToTableCol :: forall m a . Applicative m => (forall b . String -> m b) ->
  ((forall b . Int      -> String -- ^ expected type
                        -> String -- ^ given type
                        -> m b) -> a -> Maybe (Vector a -> m TableCol))
  -> Symbol -> Vector a -> m TableCol
vectorToTableCol toException inferType colname v = transformer v
  where transformer = fromMaybe (pure . replicateNone) $ join $ V.find isJust $ inferType err <$> v
        transformer :: Vector a -> m TableCol
        err rowId expected given = toException $ context <> ", Row " <> show (succ rowId)
          <> ": expected '" <> expected <> "' got " <> given
        err :: Int -> String -> String -> m b
        context = "Column " <> toS colname
{-# INLINABLE vectorToTableCol #-}

wrap :: Wrappable a => (Int -> b -> IO (Maybe a)) -> Vector b -> IO TableCol
wrap f v = toWrappedDyn <$> V.imapM f v

type Parametric f = (forall a b. (Coercible a b => Coercible (f a) (f b)) :: Constraint)

newtype Converter f = Converter { getConverter :: forall a g . (Wrappable2 g a, Parametric g)
  => R.TypeRep a -> Maybe (f (g a) -> WrappedDyn f) }

{-# INLINABLE wrap #-}
convertColUsingConverter :: Converter f -> Maybe Symbol -> WrappedDyn f -> WrappedDyn f
convertColUsingConverter (Converter conv) _ = \case
  (WrappedDyn (App f t) v)
    | Just HRefl <- f `eqTypeRep` typeRep @I, Just conv2 <- conv t -> conv2 v
    | Just HRefl <- f `eqTypeRep` typeRep @Maybe, Just conv2 <- conv t -> conv2 v
  c -> c
  -- (WrappedDyn tr _)     -> throwH $ TypeMismatch $ "in Column "
        -- <> maybe "<unknown>" toS n <> ": type " <> show tr <> " not supported"
{-# INLINABLE convertColUsingConverter #-}

exampleConverter :: Functor f => Converter f
exampleConverter = Converter $ \case
  t | Just HRefl <- t    `eqTypeRep` typeRep @Double -> Just $ toWrappedDyn . coerce2 @Double @Double
    | Just HRefl <- t    `eqTypeRep` typeRep @Double -> Just $ toWrappedDyn . fmap id
    | True -> Nothing


convertUsingConverter :: ToTableAndBack t => Converter Vector -> t -> H (InH t)
convertUsingConverter conv = useToTableAndBack $ chain $ table2
  . mapDictWithKey (convertColUsingConverter conv . Just) . flipTable
{-# INLINABLE convertUsingConverter #-}

{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Hoff.HQuery.Operators where

import           Data.Coerce
import qualified Data.HashSet as HS
import qualified Data.Maybe as P
import qualified Data.Set as S
import qualified Data.Text as T
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict
import           Hoff.H as H
import           Hoff.HQuery.Execution as H
import           Hoff.HQuery.Expressions as H
import           Hoff.HQuery.TH as H
import           Hoff.Iterable as H
import           Hoff.Table
import           Hoff.Vector
import qualified Prelude as P
import           Text.InterpolatedString.Perl6
import           Yahp hiding (liftA, delete, partition)

-- * Zipping

zipD :: forall f g . Zippable (f :.: g) => Zippable (f :.: g) => HQueryDyn f g -> HQueryDyn f g
     -> HQueryDyn f g
zipD = coerce $ liftA2 @(NamedTableReader f) $ zipDyn @(f :.: g)
{-# INLINE zipD #-}

zip3D :: forall f g . Zippable (f :.: g) => HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g
      -> HQueryDyn f g
zip3D = coerce $ liftA3 @(NamedTableReader f) $ zip3Dyn @(f :.: g)
{-# INLINE zip3D #-}

zip4D :: forall f g . Zippable (f :.: g) => HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g
      -> HQueryDyn f g
zip4D = coerce $ liftA4 @(NamedTableReader f) $ zip4Dyn @(f :.: g)
{-# INLINE zip4D #-}

zip5D :: forall f g . Zippable (f :.: g) => HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g
      -> HQueryDyn f g -> HQueryDyn f g
zip5D = coerce $ liftA5 @(NamedTableReader f) $ zip5Dyn @(f :.: g)
{-# INLINE zip5D #-}

zip6D :: forall f g . Zippable (f :.: g) => HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g
      -> HQueryDyn f g -> HQueryDyn f g -> HQueryDyn f g
zip6D = coerce $ liftA6 @(NamedTableReader f) $ zip6Dyn @(f :.: g)
{-# INLINE zip6D #-}


-- * Misc

betweenII :: (Ord a, Zippable f, Zippable g)
  => HQuery f g a -> HQuery f g a -> HQuery f g a -> HQuery f g Bool
betweenII = zipWith3_ $ \a b c -> b <= a && a <= c
{-# INLINABLE betweenII #-}

betweenIE :: (Ord a, Zippable f, Zippable g)
  => HQuery f g a -> HQuery f g a -> HQuery f g a -> HQuery f g Bool
betweenIE = zipWith3_ $ \a b c -> b <= a && a < c
{-# INLINE betweenIE #-}

betweenEI :: (Ord a, Zippable f, Zippable g)
  => HQuery f g a -> HQuery f g a -> HQuery f g a -> HQuery f g Bool
betweenEI = zipWith3_ $ \a b c -> b < a && a <= c
{-# INLINABLE betweenEI #-}

betweenEE :: (Ord a, Zippable f, Zippable g)
  => HQuery f g a -> HQuery f g a -> HQuery f g a -> HQuery f g Bool
betweenEE = zipWith3_ $ \a b c -> b < a && a < c
{-# INLINABLE betweenEE #-}

bool_ :: forall x f g . (Zippable f, Zippable g)
  => HQuery f g x -> HQuery f g x -> HQuery f g Bool -> HQuery f g x
bool_ = zipWith3_ bool
{-# INLINABLE bool_ #-}

rowDict :: HQuery f Vector TableRowDict
rowDict = noName $ ReaderT $ \t -> gtToGroups t "a" <=< rowDicts $ gtTable t
{-# INLINABLE rowDict #-}

-- * Maybe 

isJust_ :: ColumnData2 f g => HQueryDyn f g -> HQuery f g Bool
isJust_ = liftCWithNameStatic isJustCol
{-# INLINE isJust_ #-}

isNothing_ :: ColumnData2 f g => HQueryDyn f g -> HQuery f g Bool
isNothing_ = fmap not . isJust_
{-# INLINE isNothing_ #-}

fromJust_ :: forall c g f . (Functor g, Functor f) => HasCallStack => HQuery f g (P.Maybe c) -> HQuery f g c
fromJust_ = fmap P.fromJust
{-# INLINE fromJust_ #-}

fromJustD :: (ColumnData2 f g, HasCallStack) => HQueryDyn f g -> HQueryDyn f g
fromJustD = liftCWithNameDyn (fromJustCol Nothing)
{-# INLINE fromJustD #-}

fromMaybe_ :: forall c f . Functor f => c -> Exp f (P.Maybe c) -> Exp f c
fromMaybe_ = fmap . P.fromMaybe
{-# INLINE fromMaybe_ #-}

toM :: (ColumnData2 f g, HasCallStack) => HQueryDyn f g -> HQueryDyn f g
toM = liftCWithNameDyn toMaybe
{-# INLINE toM #-}

-- * Either


isRight_ :: (Functor f, HasCallStack) => ExpDyn f -> Exp f Bool
isRight_ = liftC' isRightCol
{-# INLINE isRight_ #-}


-- * Aggregators

countA :: ICoerce f Int => Agg f Int
countA = withName "count" $ reader $ Comp . coerceI . gtGroupSizes
{-# INLINABLE countA #-}

countDistinct :: Functor f => ExpDyn f -> Agg f Int
countDistinct = mapD0 f
  where f :: forall g a . (Wrappable a, Wrappable1 g) => Vector (g a) -> Int
        f = length . HS.fromList . coerceWrapInstances . toList
{-# INLINABLE countDistinct #-}

-- * lookups

(!.) :: (Eq a, ExpressionTypeVector f g, Typeable a, Hashable a, Show a)
  => VectorDict a b -> HQuery f g a -> HQuery f g b
(!.) d1 = liftVecH (d1 !!) 
{-# INLINE (!.) #-}

(?.) :: (Eq a, Hashable a, ExpressionTypeVector f g)
  => VectorDict a v -> HQuery f g a -> HQuery f g (Maybe v)
(?.) d1 = liftVec (d1 !!?) 
{-# INLINE (?.) #-}

-- * instances

instance (ExpressionTypeZip f g, Fractional a) => Fractional (HQuery f g a) where
  (/)           = zipWith_ (/)
  recip         = fmap recip
  fromRational  = co . fromRational
  
instance (ExpressionTypeZip f g, Num a) => Num (HQuery f g a) where
  (+)           = zipWith_ (+)
  (*)           = zipWith_ (*)
  (-)           = zipWith_ (-)
  negate        = fmap negate
  abs           = fmap abs
  signum        = fmap signum
  fromInteger   = co . fromInteger
  
-- | these instances are needed for floating
-- 
-- But valid implementations cannot exist because HQuery is basically a function which cannot be checked
-- for equality
-- 
-- Use `==.` or `fmap (== 'a')` instead
instance {-# OVERLAPS #-} Eq (HQuery f g a) where
  (==) = notImpl2 "(==) // Use `==.` or `fmap (== 'a')` instead //"

-- | this is needed for Num
instance {-# OVERLAPS #-} Ord (HQuery f g a) where
  compare = notImpl2 "compare"

instance (Num (HQuery f g a)) => Real (HQuery f g a) where
  toRational = notImpl1 "toRational"

instance (ExpressionTypeZip f g, Floating a) => Floating (HQuery f g a) where
  pi = co pi
  (**)          = zipWith_ (**)
  logBase       = zipWith_ logBase
  cos           = fmap cos
  acosh         = fmap acosh
  asin          = fmap asin
  asinh         = fmap asinh
  atan          = fmap atan
  atanh         = fmap atanh
  acos          = fmap acos
  cosh          = fmap cosh
  exp           = fmap exp
  log           = fmap log
  sin           = fmap sin
  sinh          = fmap sinh
  sqrt          = fmap sqrt
  tan           = fmap tan
  tanh          = fmap tanh

instance (Functor f, Functor g, ConvertText a b) => ConvertText (HQuery f g a) (HQuery f g b) where
  toS = fmap toS

instance (ExpressionType f g, Functor f, Functor g, Enum a) => Enum (HQuery f g a) where
  succ          = fmap succ
  pred          = fmap pred
  toEnum        = co . toEnum
  fromEnum      = notImpl1 "fromEnum"

instance (ExpressionTypeZip f g, Integral a) => Integral (HQuery f g a) where
  quot          = zipWith_ quot   
  rem           = zipWith_ rem    
  div           = zipWith_ div    
  mod           = zipWith_ mod    
  quotRem       = notImpl2 "quotRem"
  divMod        = notImpl2 "divMod"
  toInteger     = notImpl1 "toInteger"

properFraction_ :: (RealFrac a, Integral b, Functor2 f g) => HQuery f g a -> HQuery f g (b, a)
properFraction_ = mapE Yahp.properFraction
{-# INLINABLE properFraction_ #-}

truncate_ :: (RealFrac a, Integral b, Functor2 f g) => HQuery f g a -> HQuery f g b
truncate_       = mapE Yahp.truncate
{-# INLINABLE truncate_ #-}

round_ :: (RealFrac a, Integral b, Functor2 f g) => HQuery f g a -> HQuery f g b
round_          = mapE Yahp.round
{-# INLINABLE round_ #-}

ceiling_ :: (RealFrac a, Integral b, Functor2 f g) => HQuery f g a -> HQuery f g b
ceiling_        = mapE Yahp.ceiling
{-# INLINABLE ceiling_ #-}

floor_ :: (RealFrac a, Integral b, Functor2 f g) => HQuery f g a -> HQuery f g b
floor_          = mapE Yahp.floor
{-# INLINABLE floor_ #-}

length_ :: forall v f a g . (Functor2 f g, v ~ f a, Foldable f) => HQuery f g v -> HQuery f g Int
length_ = mapE length
{-# INLINABLE length_ #-}

instance {-# OVERLAPS #-} (Zippable f, Zippable g, Semigroup a) => Semigroup (HQuery f g a) where
  (<>) = zipWith_ (<>)

instance {-# OVERLAPS #-} (ExpressionTypeZip f g, Monoid a) => Monoid (HQuery f g a) where
  mempty = co mempty

instance (ExpressionTypeZip f g, Bounded a) => Bounded (HQuery f g a) where
  minBound = co minBound
  maxBound = co maxBound

notImpl' :: HasCallStack => String -> String -> a
notImpl' b n = P.error $ "'HQuery f' has no '" <> n <> "' implementation." <> b
{-# INLINABLE notImpl' #-}

notImpl2 :: HasCallStack => String -> HQuery f a b -> HQuery g c d -> x
notImpl2 e c y = withFrozenCallStack $ notImpl' [qq| Columns: {getName c} and {getName y}|] e
{-# INLINABLE notImpl2 #-}

notImpl1 :: HasCallStack => String -> HQuery f a b -> x
notImpl1 e c = withFrozenCallStack $ notImpl' [qq| Column: {getName c}|] e
{-# INLINABLE notImpl1 #-}

-- -- * lifted

$(concatMapM (mapA1flip_ "_" . fmap Just)
   [('(V.!)             , "at_")
   ,('(V.!?)            , "lookup_")
   ])

$(concatMapM (mapA1flip_ "_" . (,Nothing))
  [ 
   ])

$(concatMapM (mapA1_ "_")
-- Vector
   ['V.all
   ,'V.any
   ,'V.maximumBy
   ,'V.minimumBy
   ])

$(concatMapM (mapA_ "A")
  ['V.maximum -- unsafe! partial! throws for empty vectors
  ,'V.minimum -- unsafe! partial! throws for empty vectors
  ,'V.last -- unsafe! partial! throws for empty vectors
  ,'V.and
  ,'V.or
  ,'V.sum
  ,'V.product
  ,'V.head -- unsafe! partial! throws for empty vectors
  ])

sumAd :: Functor f => Exp f Double -> Agg f Double
sumAd = sumA
{-# INLINE sumAd #-}

sumAf :: Functor f => Exp f Float -> Agg f Float
sumAf = sumA
{-# INLINE sumAf #-}

sumAi64 :: Functor f => Exp f Int64 -> Agg f Int64
sumAi64 = sumA
{-# INLINE sumAi64 #-}

sumAi :: Functor f => Exp f Int -> Agg f Int
sumAi = sumA
{-# INLINE sumAi #-}

  
$(concatMapM (appNonPolymorphic 'mapD1 "D")
  ['V.last
  ,'V.head
  ])

-- | elemV :: (Eq a, Functor f) => Vector a -> f a -> f Bool
$(concatMapM (liftE1flip_ (<>"V"))
  ['V.elem
   ])


elemF :: (Eq a, Foldable t, Functor f) => t a -> f a -> f Bool
elemF = fmap . flip elem

-- $(concatMapM (liftE1flip_ (<>"F"))
   -- ])

$(concatMapM (liftE1flip_ (<>"S"))
  ['S.member
   ])

-- | (.==) :: (Eq a, Functor f) => a -> f a -> f Bool
$(concatMapM (liftE1_ ("." <> ))
  ['(&&)
  ,'(||)
  ,'(==)
  ,'(/=)
  ,'(<)
  ,'(<=)
  ,'(>)
  ,'(>=)
  ,'(+)
  ,'(-)
  ,'(*)
   ])

-- | (==.) :: (Eq a, Zippable f) => f a -> f a -> f Bool
$(concatMapM (zipWithTh "." . (,Nothing))
  ['(&&)
  ,'(||)
  ,'(==)
  ,'(/=)
  ,'(<)
  ,'(<=)
  ,'(>)
  ,'(>=)
  ,'(+)
  ,'(-)
  ,'(*)
  ])

$(concatMapM (zipWithTh "_" . (,Nothing))
-- Prelude
  ['max
  ,'min
  ,'quotRem
  ,'divMod
  ,'compare
  -- Text
  ,'T.cons
  ,'T.snoc
  ,'T.append
  ])

$(concatMapM (zipWithTh "" . fmap Just)
  [
  ])


$(concatMapM (appNonPolymorphic 'mapE "T")
-- Text
  ['T.pack
  ,'T.unpack
  ,'T.singleton
  ,'T.uncons
  ,'T.unsnoc
  ,'T.head
  ,'T.last
  ,'T.tail
  ,'T.init
  ,'T.null
  ,'T.length
  ,'T.transpose
  ,'T.reverse
  ,'T.toCaseFold
  ,'T.toLower
  ,'T.toUpper
  ,'T.toTitle
  ,'T.strip
  ,'T.stripStart
  ,'T.stripEnd
  ,'T.lines
  ,'T.words
  ,'T.unlines
  ,'T.unwords
  ])

replaceT :: Zippable (HQuery f g) => Text -> HQuery f g Text -> HQuery f g Text -> HQuery f g Text
replaceT x = zipWith_ $ T.replace x
{-# INLINE replaceT #-}

zipWithT :: Zippable (HQuery f g)
  => (Char -> Char -> Char) -> HQuery f g Text -> HQuery f g Text -> HQuery f g Text
zipWithT x = zipWith_ $ T.zipWith x
{-# INLINE zipWithT #-}

$(concatMapM (appNonPolymorphic 'liftE1 "T")
-- Text
  ['T.intercalate
  ,'T.compareLength
  ,'T.intersperse
  ,'T.take
  ,'T.takeEnd
  ,'T.drop
  ,'T.dropEnd
  ,'T.takeWhile
  ,'T.takeWhileEnd
  ,'T.dropWhile
  ,'T.dropWhileEnd
  ,'T.dropAround
  ,'T.splitAt
  ,'T.breakOn
  ,'T.breakOnEnd
  ,'T.break
  ,'T.span
  ,'T.split
  ,'T.chunksOf
  ,'T.isPrefixOf
  ,'T.isSuffixOf
  ,'T.isInfixOf
  ,'T.stripPrefix
  ,'T.stripSuffix
  ,'T.commonPrefixes
  ,'T.filter
  ,'T.find
  ,'T.elem
  ,'T.partition
  ,'T.index
  ,'T.findIndex
  ,'T.count
  ])

$(concatMapM (appNonPolymorphic 'zipWith_ "T")
  ['T.zip
  ])

$(concatMapM (appNonPolymorphic 'co "T")
  ['T.empty
  ])

-- * Group maximum

isGroupMaxD :: (Zippable f) => ExpDyn f -> Exp f Bool
isGroupMaxD = rawDColToStatic $ \v -> (== V.maximum v) <$> v
{-# INLINABLE isGroupMaxD #-}

isGroupMinD :: (Zippable f) => ExpDyn f -> Exp f Bool
isGroupMinD = rawDColToStatic $ \v -> (== V.minimum v) <$> v
{-# INLINABLE isGroupMinD #-}

rawDColToStatic :: (Zippable f) =>
  (forall a . (Eq a, Ord a) => Vector a -> Vector b) -> ExpDyn f -> Exp f b
rawDColToStatic f = mapHqDynStaticCol $ withWrapped $ mapComp2 $ coerceWrapInstances >>> f
{-# INLINABLE rawDColToStatic #-}

isGroupMaximum :: (Ord a, Zippable f, Eq a) => Exp f a -> Exp f Bool
isGroupMaximum e = e ==. ca (maximumA e)
{-# INLINABLE isGroupMaximum #-}

isGroupMinimum :: (Ord a, Zippable f, Eq a) => Exp f a -> Exp f Bool
isGroupMinimum e = e ==. ca (minimumA e)
{-# INLINABLE isGroupMinimum #-}

rowsWithGroupMaximum :: (HasCallStack, ToTableAndBack t) => ExpDyn Vector -> ExpDyns I -> t -> H' t
rowsWithGroupMaximum = filterBy . isGroupMaxD
{-# INLINABLE rowsWithGroupMaximum #-}

rowsWithGroupMinimum :: (HasCallStack, ToTableAndBack t) => ExpDyn Vector -> ExpDyns I -> t -> H' t
rowsWithGroupMinimum = filterBy . isGroupMinD
{-# INLINABLE rowsWithGroupMinimum #-}


-- * Window

-- ** prev / next
prevD :: (ColumnData2 f g, ExpressionTypeVector f g, HasCallStack) => HQueryDyn f g -> HQueryDyn f g
prevD = liftCWithNameDyn $ modifyMaybeCol $ Just $ TableColModifierWithDefault $ \_ -> mapVector . prevV
{-# INLINABLE prevD #-}

nextD :: (ColumnData2 f g, ExpressionTypeVector f g, HasCallStack) => HQueryDyn f g -> HQueryDyn f g
nextD = liftCWithNameDyn $ modifyMaybeCol $ Just $ TableColModifierWithDefault $ \_ -> mapVector . nextV
{-# INLINABLE nextD #-}

prev :: forall a f g . ExpressionTypeVector f g => HQuery f g a -> HQuery f g (Maybe a)
prev = liftVec $ prevV Nothing . fmap Just
{-# INLINABLE prev #-}

next :: forall a f g . ExpressionTypeVector f g => HQuery f g a -> HQuery f g (Maybe a)
next = liftVec $ nextV Nothing . fmap Just
{-# INLINABLE next #-}

prevM :: forall a f g . ExpressionTypeVector f g => HQuery f g (Maybe a) -> HQuery f g (Maybe a)
prevM = liftVec $ prevV Nothing
{-# INLINABLE prevM #-}

nextM :: forall a f g . ExpressionTypeVector f g => HQuery f g (Maybe a) -> HQuery f g (Maybe a)
nextM = liftVec $ nextV Nothing
{-# INLINABLE nextM #-}

-- ** forward / backward fill
--
-- resulting types are also Maybe for cases where first (last) element is missing

ffillD :: (ColumnData2 f g, ExpressionTypeVector f g, HasCallStack) => HQueryDyn f g -> HQueryDyn f g
ffillD = liftCWithNameDyn $ modifyMaybeCol $ Just $ TableColModifierWithDefault $ fmap2 mapVector ffillGeneric
{-# INLINABLE ffillD #-}

bfillD :: (ColumnData2 f g, ExpressionTypeVector f g, HasCallStack) => HQueryDyn f g -> HQueryDyn f g
bfillD = liftCWithNameDyn $ modifyMaybeCol $ Just $ TableColModifierWithDefault $ fmap2 mapVector bfillGeneric
{-# INLINABLE bfillD #-}

ffill :: forall a f g . ExpressionTypeVector f g => HQuery f g (Maybe a) -> HQuery f g (Maybe a)
ffill = liftVec ffillV
{-# INLINABLE ffill #-}

bfill :: forall a f g . ExpressionTypeVector f g => HQuery f g (Maybe a) -> HQuery f g (Maybe a)
bfill = liftVec bfillV
{-# INLINABLE bfill #-}

ffillDev :: forall a f g . ExpressionTypeVector f g => a -> HQuery f g (Maybe a) -> HQuery f g a
ffillDev = liftVec . ffillDefV
{-# INLINABLE ffillDev #-}

bfillDev :: forall a f g . ExpressionTypeVector f g => a -> HQuery f g (Maybe a) -> HQuery f g a
bfillDev = liftVec . bfillDefV
{-# INLINABLE bfillDev #-}

-- ** cumulative sum and product
csum :: (Num b, ExpressionTypeVector f g) => HQuery f g b -> HQuery f g b
csum = liftVec csumV
{-# INLINE csum #-}

cproduct :: (Num b, ExpressionTypeVector f g) => HQuery f g b -> HQuery f g b
cproduct = liftVec cproductV
{-# INLINE cproduct #-}

-- * Table operations (contained in this file because they use HQuery.Execution)

-- ** Melt

melt :: (HasCallStack, ToTable t)
  => [Symbol]    -- ^ list of columns to melt. they have to have the same type
  -> Symbol             -- ^ name of the new column containing the column names
  -> Symbol             -- ^ name of the new column containing the column values
  -> t -> TableH
melt cols measureColName valueColName = chainToH @Table $ \t -> wrapErrorMeta t $ do
  neCols <- noteH (TableWithNoColumn "melt needs non-empty list of columns to melt") $ nonEmpty cols
  t2 <- delete cols t
  concatMatchingTables <=< forM neCols $ \cn -> getTableColWithoutWrapErrorMeta cn t
    >>= \cv -> update [ci measureColName cn, tcoli valueColName $ toSingleGroupTableCol cv] t2


-- ** Either

-- | (Lefts, Rights)
-- Requires explicit left type to prove constraint
partitionEithers_ :: (HasCallStack, ToTable t, Wrappable left) => Proxy left -> Symbol -> t
  -> H (Table, Table)
partitionEithers_ p col = chainToH @Table $ \t -> do
  gs <- groupByCol (isRight_ colE) t
  let up f x = update [liftC f colE] $ fromMaybe (H.take 0 t) $ gs !? x
  (,) <$> up (fromLeftColUnsafe p) False <*> up fromRightColUnsafe True
  where colE = tableColD col
{-# INLINE partitionEithers_ #-}

lefts_ :: (ToTable t, Wrappable left) => Proxy left -> Symbol -> t -> TableH
lefts_ p c = fmap fst . partitionEithers_ p c
{-# INLINABLE lefts_ #-}

rights_ :: ToTable t => Symbol -> t -> TableH
rights_ c = fmap snd . partitionEithers_ (Proxy @()) c
{-# INLINABLE rights_ #-}

-- * Grouping and windowing

groupByCol :: forall a t . (ToTableAndBack t, Eq a, Hashable a) => Exp I a -> t -> VectorDictH a (InH t)
groupByCol by t1 = do
  t <- t2
  unsafeValue_ (traverse $ back . pure) =<< (flip groupBy_ t <$> exec by t)
  where (t2, back) = toTableAndBack t1
{-# INLINABLE groupByCol #-}


partition :: (HasCallStack, ToTable t) => Where -> t -> H (Table, Table)
-- ^ rows that evaluate to (True, False)
partition wh = chainToH $ \t -> runWhere wh t >>= \w ->
  (,) <$> applyWhereResult t w <*> applyWhereResult t (invert w)
{-# INLINABLE partition #-}


-- | (Rows with at least one Nothing-value in given columns
--   , Table with remaining rows and `fromJust`ed or untouched columns)
partitionMaybes3 :: (HasCallStack, ToTable t) => Bool -> Symbols -> t -> H (Table, Table)
partitionMaybes3 performFromJust cols = chainToH $ \t -> bool (g t) (pure (H.take 0 t, t)) $ V.null cols
  where
    g = mapM (bool pure (update $ fromJustD . ad <$> toList cols) performFromJust)
      <=< partition (V.foldl1 (zipWith_ (||)) $ isNothing_ . ad <$> cols)
{-# INLINABLE partitionMaybes3 #-}

-- | (Rows with at least one Nothing-value in given columns
--   , Table with `fromJust`ed columns)
partitionMaybes2 :: (HasCallStack, ToTable t) => Symbols -> t -> H (Table, Table)
partitionMaybes2 = partitionMaybes3 True
{-# INLINE partitionMaybes2 #-}

-- | (Table with rows that contained nothing in the given column but with that column deleted
--   , Table with `fromJust`ed column)
partitionMaybes :: (HasCallStack, ToTable t) => Symbol -> t -> H (Table, Table)
partitionMaybes col = chainToH $ \t -> do
  whereJust <- runWhere (isJust_ $ ad col) t
  (,) <$> (delete @Table [col] =<< applyWhereResult t (invert whereJust))
    <*> update [fromJustD $ ad col] (applyWhereResult t whereJust)
{-# INLINE partitionMaybes #-}
        
-- | return rows where given columns are Just x and cast to x
justs2 :: ToTable t => Symbols -> t -> TableH
justs2           = fmap3 snd partitionMaybes2
{-# INLINABLE justs2 #-}

-- | return rows where column is Just x and cast to x
justs :: ToTable t => Symbol -> t -> TableH
justs           = fmap3 snd partitionMaybes
{-# INLINABLE justs #-}

-- | return rows where column is Nothing and delete the column
nothings2 :: ToTable t => Symbols -> t -> TableH
nothings2        = fmap3 fst partitionMaybes2
{-# INLINABLE nothings2 #-}

-- | return rows where column is Nothing and delete the column
nothings :: ToTable t => Symbol -> t -> TableH
nothings        = fmap3 fst partitionMaybes
{-# INLINABLE nothings #-}

-- | for each group, distribute the aggregated value back to the whole group
-- 
-- see also `ca` for a full-table-aggregate version
fby :: forall a f . (Show a, Show1 f, HasCallStack) => Agg Vector a -> ExpDyns I -> Exp f a
fby = flip $ \bys -> mapNTR $ \agg -> ReaderT $ \(primary, _) -> do
  secondaryIdxs <- snd . getGroupsAndGrouping <$> select bys (gtTable primary)
  gtToGroups primary "fby" . broadcastGroupValue secondaryIdxs . coerce' @(Vector a)
    =<< runReaderT agg (fromGrouping secondaryIdxs $ gtTable primary, [])
{-# INLINABLE fby #-}

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
--   expVs <- unsafeApplyGrouping idxs <$> runReaderT exp t
--   let resVs = f <$> expVs
--   if fmap length expVs == fmap length resVs then
--     pure $ unsafeBackpermute (V.concatMap id idxs) $ V.concatMap id resVs
--     else throwH $ CountMismatch $ unlines $ "Function returned vector of different length:\n" :
--          toList (V.zipWith (\a b -> show (length a, length b)) expVs (V.take 10 resVs))
-- {-# INLINE eby #-}


checkDuplicates :: ToTable t => Bool -> ExpDyns I -> ExpDyn I -> t -> H Table
checkDuplicates doThrow gby sby tbl = do
  dups <- ascTD sby $ H.filter ((> (1 :: Int)) <$> #count) $ updateBy [ai $ ca countA] gby $ toTable tbl
  (dups <$) $ when (doThrow && not (H.null dups)) $ withFrozenCallStack
    $ throwH $ DuplicatesError $ "\n" <> show dups <> "\nThere where the above duplicates"
{-# INLINABLE checkDuplicates #-}

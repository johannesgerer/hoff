{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Hoff.HQuery.Operators where

import           Data.Coerce
import qualified Data.HashSet as HS
import qualified Data.Maybe as P
import qualified Data.Text as T
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict
import           Hoff.H as H
import           Hoff.HQuery as H
import           Hoff.HQuery.TH as H
import           Hoff.Iterable as H
import           Hoff.Table
import           Hoff.Vector
import qualified Prelude as Unsafe
import           Yahp hiding (liftA, delete)

-- * Zipping

zipD :: forall t . HQueryDyn t -> HQueryDyn t -> HQueryDyn t
zipD = coerce $ liftA2 @(NamedTableReader t) zipDyn
{-# INLINE zipD #-}

zip3D :: forall t . HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t
zip3D = coerce $ liftA3 @(NamedTableReader t) zip3Dyn
{-# INLINE zip3D #-}

zip4D :: forall t . HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t
zip4D = coerce $ liftA4 @(NamedTableReader t) zip4Dyn
{-# INLINE zip4D #-}

zip5D :: forall t . HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t
zip5D = coerce $ liftA5 @(NamedTableReader t) zip5Dyn
{-# INLINE zip5D #-}

zip6D :: forall t . HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t -> HQueryDyn t
zip6D = coerce $ liftA6 @(NamedTableReader t) zip6Dyn
{-# INLINE zip6D #-}

zip_ :: forall a b t . HQuery t a -> HQuery t b -> HQuery t (a, b)
zip_ = liftE2 V.zip
{-# INLINE zip_ #-}

zip3_ :: forall a b c t . HQuery t a -> HQuery t b -> HQuery t c -> HQuery t (a, b, c)
zip3_ = liftE3 V.zip3
{-# INLINE zip3_ #-}

zip4_ :: forall a b c d t . HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t (a, b, c, d)
zip4_ = liftE4 V.zip4
{-# INLINE zip4_ #-}

zip5_ :: forall a b c d e t . HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e
  -> HQuery t (a, b, c, d, e)
zip5_ = liftE5 V.zip5
{-# INLINE zip5_ #-}

zip6_ :: forall a b c d e f t . HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e -> HQuery t f
  -> HQuery t (a, b, c, d, e, f)
zip6_ = liftE6 V.zip6
{-# INLINE zip6_ #-}

zipWith_ :: (a -> b -> c) -> HQuery t a -> HQuery t b -> HQuery t c
zipWith_ = zipWith_internal
{-# INLINE zipWith_ #-}

zipWith3_ :: (a -> b -> c -> x) -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t x
zipWith3_ = liftE3 . V.zipWith3
{-# INLINE zipWith3_ #-}

zipWith4_ :: (a -> b -> c -> d -> x) -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t x
zipWith4_ = liftE4 . V.zipWith4
{-# INLINE zipWith4_ #-}

zipWith5_ :: (a -> b -> c -> d -> e -> x)
  -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e -> HQuery t x
zipWith5_ = liftE5 . V.zipWith5
{-# INLINE zipWith5_ #-}

zipWith6_ :: (a -> b -> c -> d -> e -> f -> x)
  -> HQuery t a -> HQuery t b -> HQuery t c -> HQuery t d -> HQuery t e -> HQuery t f -> HQuery t x
zipWith6_ = liftE6 . V.zipWith6
{-# INLINE zipWith6_ #-}

-- * Misc

betweenII :: Ord a => HQuery t a -> HQuery t a -> HQuery t a -> HQuery t Bool
betweenII = zipWith3_ $ \a b c -> b <= a && a <= c
{-# INLINABLE betweenII #-}

betweenIE :: Ord a => HQuery t a -> HQuery t a -> HQuery t a -> HQuery t Bool
betweenIE = zipWith3_ $ \a b c -> b <= a && a < c
{-# INLINE betweenIE #-}

betweenEI :: Ord a => HQuery t a -> HQuery t a -> HQuery t a -> HQuery t Bool
betweenEI = zipWith3_ $ \a b c -> b < a && a <= c
{-# INLINABLE betweenEI #-}

betweenEE :: Ord a => HQuery t a -> HQuery t a -> HQuery t a -> HQuery t Bool
betweenEE = zipWith3_ $ \a b c -> b < a && a < c
{-# INLINABLE betweenEE #-}

bool_ :: HQuery t Bool -> HQuery t x -> HQuery t x -> HQuery t x
bool_ = liftE3 conditional
{-# INLINABLE bool_ #-}

rowDict :: Exp TableRowDict
rowDict = noName $ ReaderT rowDicts

-- * Maybe 
fromJust_ :: HasCallStack => Exp (P.Maybe c) -> Exp c
fromJust_ = fmap P.fromJust
{-# INLINABLE fromJust_ #-}

fromJustD :: HasCallStack => HQueryDyn t -> HQueryDyn t
fromJustD = HQueryDyn . liftCWithName (fromJustCol Nothing)
{-# INLINABLE fromJustD #-}

fromMaybe_ :: Functor f => c -> Exp f (P.Maybe c) -> Exp f c
fromMaybe_ = fmap . P.fromMaybe
{-# INLINABLE fromMaybe_ #-}

toM :: HQueryDyn f -> HQueryDyn f
toM = HQueryDyn . liftCWithName toMaybe
{-# INLINABLE toM #-}

-- * extract important groups (from Maybe and Either cols)

groupByCol :: forall t a . (ToTableAndBack t, Eq a, Hashable a) => Exp a -> t -> VectorDictH a (InH t)
groupByCol by t1 = do
  t <- t2
  unsafeValue_ (traverse $ back . pure) =<< (flip groupBy_ t <$> exec by t)
  where (t2, back) = toTableAndBack t1
{-# INLINABLE groupByCol #-}

-- | (Lefts, Rights)
-- Requires explicit left type to prove constraint
partitionEithers_ :: (ToTable t, Wrappable left) => Proxy left -> Symbol -> t -> H (Table, Table)
partitionEithers_ p col = chainToH $ \t -> do
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

isRight_ :: ExpDyn -> Exp Bool
isRight_ = liftC' isRightCol

-- | (Rows with at least one Nothing-value in given columns
--   , Table with `fromJust`ed columns)
partitionMaybes2 :: (HasCallStack, ToTable t) => Symbols -> t -> H (Table, Table)
partitionMaybes2 cols = chainToH g
  where g t 
          | V.null cols = pure (H.take 0 t, t)
          | True = do
              whereJust <- runWhere (V.foldl1 (zipWith_ (&&)) $ isJust_ . ad <$> cols) t
              (,) <$> applyWhereResult t (invert whereJust)
                <*> update (fromJustD . ad <$> toList cols) (applyWhereResult t whereJust)
{-# INLINE partitionMaybes2 #-}

-- | (Table with rows that contained nothing in the given column but with that column deleted
--   , Table with `fromJust`ed column)
partitionMaybes :: (HasCallStack, ToTable t) => Symbol -> t -> H (Table, Table)
partitionMaybes col = chainToH $ \t -> do
  whereJust <- runWhere (isJust_ $ ad col) t
  (,) <$> (delete @Table [col] =<< applyWhereResult t (invert whereJust))
    <*> update [fromJustD $ ad col] (applyWhereResult t whereJust)
{-# INLINE partitionMaybes #-}
        

isJust_ :: ExpDyn -> Exp Bool
isJust_ = liftCWithName' isJustCol

isNothing_ :: ExpDyn -> Exp Bool
isNothing_ = fmap not . isJust_

-- | return rows where column is Just x and cast to x
justs :: ToTable t => Symbol -> t -> TableH
justs c         = fmap snd . partitionMaybes c
{-# INLINABLE justs #-}

-- | return rows where column is Nothing and delete the column
nothings :: ToTable t => Symbol -> t -> TableH
nothings c      = fmap fst . partitionMaybes c
{-# INLINABLE nothings #-}

-- * Aggregators

countA :: Agg Int
countA = withName "count" $ reader gtGroupSizes

countDistinct :: ExpDyn -> Agg Int
countDistinct = mapD0 f
  where f :: forall g a . (Wrappable a, Wrappable1 g) => Vector (g a) -> Int
        f = length . HS.fromList . coerceWrapInstances . toList

-- * lookups

(!.) :: (Eq a, Typeable a, Hashable a, Show a) => VectorDict a b -> HQuery t a -> HQuery t b
(!.) d1 = liftEH (d1 !!) 

(?.) :: (Eq a, Hashable a) => VectorDict a v -> HQuery t a -> HQuery t (Maybe v)
(?.) d1 = liftE (d1 !!?) 

-- * instances

instance (HQueryContext f, Fractional a) => Fractional (HQuery f a) where
  (/)           = zipWith_ (/)
  recip         = fmap recip
  fromRational  = co . fromRational
  
instance (HQueryContext f, Num a) => Num (HQuery f a) where
  (+)           = zipWith_ (+)
  (*)           = zipWith_ (*)
  (-)           = zipWith_ (-)
  negate        = fmap negate
  abs           = fmap abs
  signum        = fmap signum
  fromInteger   = co . fromInteger
  
-- | these instances are needed for num
-- 
-- But they do not exist, because HQuery is basically a function which cannot be checked for equality
-- 
-- Use `==.` or `fmap (== 'a')` instead
instance {-# OVERLAPS #-} Eq (HQuery f a) where
  (==) = notImpl' " Use `==.` or `fmap (== 'a')` instead." "(==)" 

instance {-# OVERLAPS #-} Ord (HQuery f a) where
  compare = notImpl "compare"

instance (HQueryContext f, Num a) => Real (HQuery f a) where
  toRational = notImpl "toRational"

instance (HQueryContext f, Floating a) => Floating (HQuery f a) where
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

instance ConvertText a b => ConvertText (HQuery f a) (HQuery f b) where
  toS = fmap toS

instance (HQueryContext f, Enum a) => Enum (HQuery f a) where
  succ          = fmap succ
  pred          = fmap pred
  toEnum        = co . toEnum
  fromEnum      = notImpl "fromEnum"

instance (HQueryContext f, Integral a) => Integral (HQuery f a) where
  quot          = zipWith_ quot   
  rem           = zipWith_ rem    
  div           = zipWith_ div    
  mod           = zipWith_ mod    
  quotRem       = notImpl "quotRem"
  divMod        = notImpl "divMod"
  toInteger     = notImpl "toInteger"

properFraction_ :: (RealFrac a, Integral b) => HQuery t a -> HQuery t (b, a)
properFraction_ = mapE Yahp.properFraction

truncate_ :: (RealFrac a, Integral b) => HQuery t a -> HQuery t b
truncate_       = mapE Yahp.truncate

round_ :: (RealFrac a, Integral b) => HQuery t a -> HQuery t b
round_          = mapE Yahp.round

ceiling_ :: (RealFrac a, Integral b) => HQuery t a -> HQuery t b
ceiling_        = mapE Yahp.ceiling

floor_ :: (RealFrac a, Integral b) => HQuery t a -> HQuery t b
floor_          = mapE Yahp.floor

length_ :: forall v f a t . (v ~ f a, Foldable f) => HQuery t v -> HQuery t Int
length_ = mapE length

instance {-# OVERLAPS #-} Semigroup a => Semigroup (HQuery f a) where
  (<>) = zipWith_ (<>)

instance {-# OVERLAPS #-} (HQueryContext f, Monoid a) => Monoid (HQuery f a) where
  mempty = co mempty

instance (HQueryContext f, Bounded a) => Bounded (HQuery f a) where
  minBound = co minBound
  maxBound = co maxBound

notImpl' :: HasCallStack => String -> String -> a
notImpl' b n = Unsafe.error $ "'HQuery f' has no " <> n <> " implementation." <> b

notImpl = notImpl' ""

-- * lifted

$(concatMapM (mapA1flip_ "_" . fmap Just)
   [('(V.!)             , "at_")
   ,('(V.!?)            , "lookup_")
   ])

-- $(concatMapM (mapA1flip_ "_" . (,Nothing))
-- [ 
--    ])

$(concatMapM (mapA1_ "_")
-- Vector
   ['V.all
   ,'V.any
   ,'V.maximumBy
   ,'V.minimumBy
   ])

$(concatMapM (mapA_ "A")
  ['V.maximum
  ,'V.minimum
  ,'V.last
  ,'V.and
  ,'V.or
  ,'V.sum
  ,'V.product
  ,'V.head])
  
$(concatMapM (appFs 'mapD1 "D")
  ['V.last
  ,'V.head])

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


$(concatMapM (appFs 'mapE "T")
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

replaceT :: Text -> HQuery t Text -> HQuery t Text -> HQuery t Text
replaceT x = zipWith_ $ T.replace x

zipWithT :: (Char -> Char -> Char) -> HQuery t Text -> HQuery t Text -> HQuery t Text
zipWithT x = zipWith_ $ T.zipWith x

$(concatMapM (appFs 'liftE1 "T")
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

$(concatMapM (appFs 'zipWith_ "T")
  ['T.zip
  ])

$(concatMapM (appFs 'co "T")
  ['T.empty
  ])


rowsWithGroupMaximum :: (HasCallStack, ToH Table a) => ExpDyn -> ExpDyns -> a -> KeyedTableH
rowsWithGroupMaximum orderBy groupBy = firstRowPerGroup groupBy . descTD orderBy
{-# INLINABLE rowsWithGroupMaximum #-}

rowsWithGroupMinimum :: (HasCallStack, ToH Table a) => ExpDyn -> ExpDyns -> a -> KeyedTableH
rowsWithGroupMinimum orderBy groupBy = firstRowPerGroup groupBy . ascTD orderBy
{-# INLINABLE rowsWithGroupMinimum #-}

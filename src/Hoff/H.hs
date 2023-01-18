{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE TemplateHaskell #-}

module Hoff.H where

import           Control.Lens
import qualified Prelude
import           TextShow hiding (fromString)
import           Yahp


-- * Exceptions 

data HoffException where
  TableWithNoColumn             :: HasCallStack => String -> HoffException
  CountMismatch                 :: HasCallStack => String -> HoffException
  TypeMismatch                  :: HasCallStack => String -> HoffException
  TableDifferentColumnLenghts   :: HasCallStack => String -> HoffException
  IncompatibleKeys              :: HasCallStack => String -> HoffException
  IncompatibleTables            :: HasCallStack => String -> HoffException
  KeyNotFound                   :: HasCallStack => String -> HoffException
  UnexpectedNulls               :: HasCallStack => String -> HoffException
  SqliteError                   :: HasCallStack => String -> HoffException
  SqlServerError                :: HasCallStack => String -> HoffException
  DuplicatesError               :: HasCallStack => String -> HoffException
  -- deriving (Eq, Generic, Typeable)

appendMsg :: String -> HoffException -> HoffException
appendMsg m = \case
  TableWithNoColumn             t -> withFrozenCallStack $ TableWithNoColumn             $ t <> m
  CountMismatch                 t -> withFrozenCallStack $ CountMismatch                 $ t <> m
  TypeMismatch                  t -> withFrozenCallStack $ TypeMismatch                  $ t <> m
  TableDifferentColumnLenghts   t -> withFrozenCallStack $ TableDifferentColumnLenghts   $ t <> m
  IncompatibleKeys              t -> withFrozenCallStack $ IncompatibleKeys              $ t <> m
  IncompatibleTables            t -> withFrozenCallStack $ IncompatibleTables            $ t <> m
  KeyNotFound                   t -> withFrozenCallStack $ KeyNotFound                   $ t <> m
  UnexpectedNulls               t -> withFrozenCallStack $ UnexpectedNulls               $ t <> m
  SqliteError                   t -> withFrozenCallStack $ SqliteError                   $ t <> m
  SqlServerError                t -> withFrozenCallStack $ SqlServerError                $ t <> m
  DuplicatesError               t -> withFrozenCallStack $ DuplicatesError               $ t <> m


instance Exception HoffException

instance TextShow HoffException where
  showb = fromString . show

instance Show HoffException where
  show = \case 
    TableWithNoColumn           t       -> "TableWithNoColumn "         <> t <> "\n" <> prettyCallStack callStack 
    CountMismatch               t       -> "CountMismatch "             <> t <> "\n" <> prettyCallStack callStack
    TypeMismatch                t       -> "TypeMismatch "              <> t <> "\n" <> prettyCallStack callStack
    TableDifferentColumnLenghts t       -> "TableDifferentColumnLenghts\n" <> t <> "\n" <> prettyCallStack callStack
    IncompatibleKeys            t       -> "IncompatibleKeys "          <> t <> "\n" <> prettyCallStack callStack
    IncompatibleTables          t       -> "IncompatibleTables "        <> t <> "\n" <> prettyCallStack callStack
    KeyNotFound                 t       -> "KeyNotFound "               <> t <> "\n" <> prettyCallStack callStack
    UnexpectedNulls             t       -> "UnexpectedNulls "           <> t <> "\n" <> prettyCallStack callStack
    SqliteError                 t       -> "SqliteError "               <> t <> "\n" <> prettyCallStack callStack
    SqlServerError              t       -> "SqlServerError "            <> t <> "\n" <> prettyCallStack callStack
    DuplicatesError             t       -> "DuplicatesError "           <> t <> "\n" <> prettyCallStack callStack


-- * Hoff Monad

newtype H a = H { hToEither :: Either HoffException a }
  deriving (Functor, Applicative, Monad, Generic)

makeLensesWithSuffix ''H

throwH :: HoffException -> H a
throwH = H . Left
{-# INLINABLE throwH #-}

guardH :: HoffException -> Bool -> H ()
guardH e = bool (throwH e) $ pure ()
{-# INLINABLE guardH #-}

mapHException :: (HoffException -> HoffException) -> H a -> H a
mapHException = (hToEither_ . _Left %~)
{-# INLINABLE mapHException #-}

noteH :: HoffException -> Maybe a -> H a
noteH e = maybe (throwH e) pure
{-# INLINABLE noteH #-}


-- ** ToH combinators

class ToH a b where
  toH :: HasCallStack => b -> H a

mapToH :: ToH v t => (v -> a) -> t -> H a
mapToH f = fmap f . toH
{-# INLINE mapToH #-}

chainToH :: ToH v t => (v -> H a) -> t -> H a
chainToH f = chain f . toH
{-# INLINE chainToH #-}

liftToH2 :: forall va vb a b c . (ToH va a, ToH vb b) => (va -> vb -> H c) -> a -> b -> H c
liftToH2 f x y = do { a <- toH x; b <- toH y; f a b }
{-# INLINABLE liftToH2 #-}

-- * Running H monad

runHWith :: (HoffException -> b) -> (a -> b) -> H a -> b
runHWith l r = either l r . hToEither
{-# INLINABLE runHWith #-}

runHEither :: H a -> Either Text a
runHEither = left shot . hToEither
{-# INLINABLE runHEither #-}

runHError :: MonadError Text m => H a -> m a
runHError = runHWith (throwError . shot) pure
{-# INLINABLE runHError #-}

runHMaybe :: H a -> Maybe a
runHMaybe = either (const Nothing) Just . hToEither
{-# INLINABLE runHMaybe #-}

unsafeH :: H a -> a
unsafeH = runHWith (throw . Prelude.userError . show) id
{-# INLINABLE unsafeH #-}

runHinIO :: H a -> IO a
runHinIO = runHorFail
{-# INLINE runHinIO #-}

runHorFail :: MonadFail m => H a -> m a
runHorFail = runHWith (fail . show) pure
{-# INLINABLE runHorFail #-}

instance TextShow a => TextShow (H a) where
  showb = runHWith showb showb

instance Show a => Show (H a) where
  show = runHWith show show

instance Eq a => Eq (H a) where
  (==) = on g hToEither
    where g (Right a) (Right b) = a == b
          g _ _ = False

-- class HoffError m where
--   throwHa :: HoffException -> m a

-- type HT = forall m . HoffError m => m (Table)

-- asds :: Table -> HT
-- asds = throwHa $ TableWithNoColumn ""

-- class ToH a b where
--   toH :: b -> H a 

-- instance {-# OVERLAPPABLE #-} ToH a a where
--   toH = pure

-- instance {-# OVERLAPS #-} ToH a (H a) where
--   toH = id
  

-- liftH2 :: (ToH x a, ToH y b) => (x -> y -> H c) -> a -> b -> H c
-- liftH2 f x y = do { a <- toH x; b <- toH y; f a b }

-- mapH :: ToH x a => (x -> c) -> a -> H c
-- mapH f = fmap f . toH

-- liftH :: ToH x a => (x -> H b) -> a -> H b
-- liftH f = chain f . toH


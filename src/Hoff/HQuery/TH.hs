{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE TemplateHaskell #-}

-- | how do develope these functions:
-- find the types of the function you are interested in and look at their TH representations:
--
-- λ> :set -XTemplateHaskell
--
-- λ> :t V.maximum
--
-- V.maximum :: Ord a => V.Vector a -> a
--
-- λ> import Text.Pretty.Simple as S

-- λ> pPrintString =<< (show <$> [t| forall a. Ord a => V.Vector a -> a |])
--
-- ForallT [ PlainTV a_10 SpecifiedSpec ]
--     [ AppT ( ConT GHC.Classes.Ord ) ( VarT a_10 ) ]
--     ( AppT
--         ( AppT ArrowT
--             ( AppT ( ConT Data.Vector.Vector ) ( VarT a_10 ) )
--         ) ( VarT a_10 )
--     )
-- --
-- λ> :t mapA V.maximum
--
-- mapA V.maximum :: (Functor f, Ord b) => Exp f b -> Agg f b
--
-- λ> S.pPrintString =<< (show <$> [t| forall f b. (Functor f, Ord b) => Exp f b -> Agg f b |])
--
-- ForallT
--     [ PlainTV f_11 SpecifiedSpec
--     , PlainTV b_12 SpecifiedSpec
--     ]
--     [ AppT ( ConT GHC.Base.Functor ) ( VarT f_11 )
--     , AppT ( ConT GHC.Classes.Ord ) ( VarT b_12 )
--     ]
--     ( AppT
--         ( AppT ArrowT
--             ( AppT
--                 ( AppT ( ConT Hoff.HQuery.Expressions.Exp ) ( VarT f_11 ) ) ( VarT b_12 )
--             )
--         )
--         ( AppT
--             ( AppT ( ConT Hoff.HQuery.Expressions.Agg ) ( VarT f_11 ) ) ( VarT b_12 )
--         )
--     )
-- --
--
-- alternatively, it is possible to reify in GHCi
-- import           Language.Haskell.TH hiding (Exp)
-- import           Language.Haskell.TH.Syntax hiding (Exp)
-- asd = fmap . (==)
-- λ> putStrLn ($(stringE . toS . pShow =<< reifyType 'asd) ::String)
--
module Hoff.HQuery.TH where

import           Hoff.HQuery.Expressions
import           Language.Haskell.TH hiding (Exp)
import           Language.Haskell.TH.Syntax hiding (Exp)
import          Text.Pretty.Simple as S
import           Yahp hiding (Type)


addCtx :: Name -> [Type] -> [Language.Haskell.TH.Syntax.Type]
addCtx constr = (<> [AppT ( ConT constr ) fVar ])

addF :: flag -> [TyVarBndr flag] -> [TyVarBndr flag]
addF x = (<> [PlainTV fName x])

fVar :: Type
fVar = VarT fName

fName :: Name
fName = mkName "f"
  
appFunc :: Quote m => Name -> Name -> Name -> m [Dec]
appFunc fn newName opn = [d| $(varP newName) = $(varE fn) $(varE opn) |]

appNonPolymorphic :: Quote m => Name -> String -> Name -> m [Dec]
appNonPolymorphic fn suf opn = appFunc fn (mkName $ nameBase opn <> suf) opn

modifyType constr n2 typ getT = case typ of
  ForallT vars ctxt t -> (:) . SigD n2 . ForallT (addF SpecifiedSpec vars) (addCtx constr ctxt) <$> getT t
  -- ForallVisT vars   t -> (:) . SigD n2 . ForallVisT (addF () vars)   <$> getT t
  _  -> pure id


mapA1flip_ :: String -> (Name, Maybe String) -> Q [Dec]
mapA1flip_ suf (n, newName) = do
  typ <- reifyType n
  let getT = \case
        (AppT (AppT ArrowT (AppT _ a)) (AppT (AppT ArrowT b) c))  ->
          pure ( AppT ( AppT ArrowT b ) ( AppT
                   ( AppT ArrowT ( AppT ( AppT ( ConT ''Exp ) fVar ) a))
                   ( AppT ( AppT ( ConT ''Agg ) fVar ) c)))
        _                                  -> fail $ show n <> " has unexcepted type " <> show typ

  modifyType ''Functor n2 typ getT <*> appFunc 'mapA1flip n2 n
  where n2 = mkName $ fromMaybe (nameBase n <> suf) newName
  
mapA1_ :: String -> Name -> Q [Dec]
mapA1_ suf n = do
  typ <- reifyType n
  let getT = \case
        (AppT (AppT ArrowT a) (AppT (AppT ArrowT (AppT _ b)) c))  ->

          pure ( AppT
                 ( AppT ArrowT a )
                 ( AppT
                   ( AppT ArrowT
                     ( AppT
                       ( AppT ( ConT ''Exp ) fVar ) b
                     )
                   )
                   ( AppT
                     ( AppT ( ConT ''Agg ) fVar ) c
                   )
                 )
               )
        _                                  -> fail $ show n <> " has unexcepted type " <> show typ

  modifyType ''Functor n2 typ getT <*> appFunc 'mapA1 n2 n
  where n2 = mkName $ nameBase n <> suf
  
mapA_ :: String -> Name -> Q [Dec]
mapA_ suf n = do
  typ <- reifyType n
  let getT = \case
        (AppT (AppT ArrowT (AppT _ a)) b)  ->
          pure ( AppT ( AppT ArrowT ( AppT ( AppT ( ConT ''Exp ) fVar ) a))
                 ( AppT ( AppT ( ConT ''Agg ) fVar ) b))
        _                                  -> fail $ show n <> " has unexcepted type " <> show typ


  modifyType ''Functor n2 typ getT <*> appFunc 'mapA n2 n
  where n2 = mkName $ nameBase n <> suf

zipWithTh :: String -> (Name, Maybe String) -> Q [Dec]
zipWithTh suf (n, newName) = do
  typ <- reifyType n

  let wrap = AppT fVar
      getT = \case (AppT (AppT ArrowT a) (AppT (AppT ArrowT b) c))  ->
                     pure $ (AppT (AppT ArrowT $ wrap a) (AppT (AppT ArrowT $ wrap b) $ wrap c))
                   t  -> fail $ show n <> " has unexcepted type " <> show t

  modifyType ''Zippable n2 typ getT <*> appFunc 'zipWith_ n2 n
  where n2 = mkName $ fromMaybe (nameBase n <> suf) newName




liftE1flip_ :: (String -> String) -> Name -> Q [Dec]
liftE1flip_ modname n = do
  typ <- reifyType n
  let getT = \case ( AppT ( AppT ArrowT a )
                     ( AppT ( AppT ArrowT b ) c)
                     )  ->
                     pure ( AppT
                            ( AppT ArrowT b )
                            ( AppT
                              ( AppT ArrowT
                                ( AppT ( VarT f ) a )
                              )
                              ( AppT ( VarT f ) c )
                            )
                          )
                   _ -> fail $ show n <> " has unexcepted type\n " <> toS(pShowNoColor typ)

  modifyType ''Functor n2 typ getT <*> appFunc 'liftE1polyFlip n2 n
  where n2 = mkName $ modname $ nameBase n
        f = mkName "f"
  
liftE1polyFlip :: Functor f => (a1 -> a2 -> b) -> a2 -> f a1 -> f b
liftE1polyFlip = (fmap .) . flip

liftE1_ :: (String -> String) -> Name -> Q [Dec]
liftE1_ modname n = do
  typ <- reifyType n
  let getT = \case ( AppT ( AppT ArrowT a )
                     ( AppT ( AppT ArrowT b ) c)
                     )  ->
                     pure ( AppT
                            ( AppT ArrowT a )
                            ( AppT
                              ( AppT ArrowT
                                ( AppT ( VarT f ) b )
                              )
                              ( AppT ( VarT f ) c )
                            )
                          )
                   _ -> fail $ show n <> " has unexcepted type " <> show typ

  modifyType ''Functor n2 typ getT <*> appFunc 'liftE1poly n2 n
  where n2 = mkName $ modname $ nameBase n
        f = mkName "f"
  
liftE1poly :: Functor f => (a1 -> a2 -> b) -> a1 -> f a2 -> f b
liftE1poly = (fmap .)


{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE TemplateHaskell #-}

module Hoff.HQuery.TH where

import qualified Data.Vector as V
import           Hoff.HQuery
import           Language.Haskell.TH hiding (Exp)
import           Yahp hiding (Type)

-- λ> putStrLn ($(stringE . show =<< reifyType 'maximum_) ::String)

appF :: Quote m => Name -> Name -> Name -> m [Dec]
appF fn newName opn = [d| $(varP newName) = $(varE fn) $(varE opn) |]

mapA1flip_ :: String -> (Name, Maybe String) -> Q [Dec]
mapA1flip_ suf (n, newName) = do
  typ <- reifyType n
  let getT = \case
        (AppT (AppT ArrowT (AppT _ a)) (AppT (AppT ArrowT b) c))  -> pure $ AppT (AppT ArrowT b)
          (AppT (AppT ArrowT (AppT (ConT ''Exp) a)) (AppT (ConT ''Agg) c))
        _                                  -> fail $ show n <> " has unexcepted type " <> show typ
  addSig <- case typ of
    ForallT vars ctxt t -> (:) . SigD n2 . ForallT vars ctxt <$> getT t
    ForallVisT vars   t -> (:) . SigD n2 . ForallVisT vars   <$> getT t
    _  -> pure id

  addSig <$> appF 'mapA1flip n2 n
  where n2 = mkName $ fromMaybe (nameBase n <> suf) newName
  
mapA1_ :: String -> Name -> Q [Dec]
mapA1_ suf n = do
  typ <- reifyType n
  let getT = \case
        (AppT (AppT ArrowT a) (AppT (AppT ArrowT (AppT _ b)) c))  -> pure $ AppT (AppT ArrowT a)
          (AppT (AppT ArrowT (AppT (ConT ''Exp) b)) (AppT (ConT ''Agg) c))
        _                                  -> fail $ show n <> " has unexcepted type " <> show typ
  addSig <- case typ of
    ForallT vars ctxt t -> (:) . SigD n2 . ForallT vars ctxt <$> getT t
    ForallVisT vars   t -> (:) . SigD n2 . ForallVisT vars   <$> getT t
    _  -> pure id

  addSig <$> appF 'mapA1 n2 n
  where n2 = mkName $ nameBase n <> suf
  

mapA_ :: String -> Name -> Q [Dec]
mapA_ suf n = do
  typ <- reifyType n
  let getT = \case
        (AppT (AppT ArrowT (AppT _ a)) b)  -> pure $ AppT (AppT ArrowT (AppT (ConT ''Exp) a))
          (AppT (ConT ''Agg) b)
        _                                  -> fail $ show n <> " has unexcepted type " <> show typ
  addSig <- case typ of
    ForallT vars ctxt t -> (:) . SigD n2 . ForallT vars ctxt <$> getT t
    ForallVisT vars   t -> (:) . SigD n2 . ForallVisT vars   <$> getT t
    _  -> pure id

  addSig <$> appF 'mapA n2 n
  where n2 = mkName $ nameBase n <> suf
  

appFs :: Quote m => Name -> String -> Name -> m [Dec]
appFs fn suf opn = appF fn (mkName $ nameBase opn <> suf) opn

zipWithTh :: String -> (Name, Maybe String) -> Q [Dec]
zipWithTh suf (n, newName) = do
  typ <- reifyType n
  let getT = \case (AppT (AppT ArrowT a) (AppT (AppT ArrowT b) c))  ->
                     let t = mkName "t"
                         tr = AppT $ AppT (ConT ''HQuery) $ VarT t
                     in pure (AppT (AppT ArrowT (tr a)) (AppT (AppT ArrowT (tr b)) (tr c))
                             ,t)
                   t  -> fail $ show n <> " has unexcepted type " <> show t
  addSig <- case typ of
    ForallT vars ctxt t -> (\(t,v) -> (:) $ SigD n2 $ ForallT (vars++[PlainTV v SpecifiedSpec]) ctxt t)
                           <$> getT t
    ForallVisT vars   t -> (\(t,v) -> (:) $ SigD n2 $ ForallVisT (vars++[PlainTV v ()]) t) <$> getT t
    _  -> pure id

  addSig <$> appF 'zipWith_internal n2 n
  where n2 = mkName $ fromMaybe (nameBase n <> suf) newName

zipWith_internal :: (a -> b -> c) -> HQuery t a -> HQuery t b -> HQuery t c
zipWith_internal = liftE2 . V.zipWith
{-# INLINE zipWith_internal #-}

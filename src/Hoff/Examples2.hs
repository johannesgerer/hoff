{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}

module Hoff.Examples2 where 


import           Data.Aeson as A
import           Data.SOP
import qualified Data.Vector as V
import           Hoff as H
import           Hoff.JSON
import qualified Prelude
import           Yahp hiding (take, group, (^), sum, lookup, error)

-- * tables

t1 :: HasCallStack => Table
t1 = unsafeH $ -- tc #a2 [u,u,u,u :: Int16] //
  #a <# [7,9,7,3 :: Double] // tc #b [10,20,40,30 :: Int64]
  // tc #c ['a','a','b','b'] // tc #d ["a","a","b","b"::Text]

t2 = t1 // tcF #e [Just 'b', Nothing, Just 'c', Nothing]

t1m = allToMaybe t1

-- * simple select

s1 = select [am @Int64 $ next #b, nextD #b,
             am @Int64 $ prev #b, prevD #b, #b, #a] t1

-- * aggregate by

ab1 = aggBy [ai $ sumA @Int64 #b, ai $ prev $ sumA @Int64 #b] #a t1


-- * selectBy

sb1 = selectBy [am @Int64 $ next #b, nextD #b,
                am @Int64 $ prev #b, prevD #b, #b, #a] #a t1

sb1s = selectBy [am @Int64 $ next #b, nextD #b,
                 am @Int64 $ prev #b, prevD #b, #b, #a] #a $ descTD #b t1

-- * rows numbers

rn1 = update [ai rn, ai rnG, ai $ ca rnA] t1

rb1 = selectBy [ai rn, ai rnG, ai $ ca rnA] #a t1

-- * external vectors

v1 = update [vi #v $ V.fromList @Char "helo"] t1
v1b = selectBy [prevD $ vi #v $ V.fromList @Char "helo"] #a t1


-- * group maximum

m1 = select [ai @Int64 $ ca $ maximumA #b, ai $ isGroupMaximum @Int64 #b
            , ai $ isGroupMaxD #b, ai $ isGroupMinD #b] t1

mg1 = updateBy [ai @Int64 $ ca $ maximumA #b, ai $ isGroupMaximum @Int64 #b
              , ai $ isGroupMaxD #b, ai $ isGroupMinD #b] #a t1

mg2 = rowsWithGroupMaximum #b #a t1

-- * fby
mf2 = updateBy [#b, ai @Int64 $ ca $ sumA #b, ai @Int64 $ fby (sumA #b) #a] #c t1

-- * fromJusts

fj1 = fromJusts [#e] t2

-- * Execute queries (i.e. from Hoff to some other Haskell datatype)

e1 = execBy (prev @(Double, Int64) $ zip_ #a #b) #a t1


-- * prev both before and after aggregation
ap2 = aggBy [ai @(V.Vector Int64) #b, suma, prevD $ suma
            , ai @Int64 $ sumA $ fromMaybe_ 0 $ prev #b] #a t1
  where suma = ai @Int64 $ sumA #b

-- * forward/backward fill

ff1 = update [#a, ffillD #a, #e, ffillD #e, bfillD #e] t2
ff2 = updateBy [#a, ffillD #a, #e, ffillD #e, bfillD #e] #c t2

ff3 = update [#e, ai $ ffillDev 'z' #e, ai $ bfillDev 'z' #e] t2
ff4 = updateBy [#e, ai $ ffillDev 'z' #e, ai $ bfillDev 'z' #e] #c t2
ff5 = update [#e, am $ ffill @Char #e, am $ bfill @Char #e] t2

-- * vector agg
ea1 = aggBy [ai $ ae @Double #a, ai $ ae $ prev @Double #a] #c t1

-- * zipA

-- | normalize:
za1 = update [#a, ai @Double $ zipA (flip (/)) (sumA #a) #a] t1
za2 = updateBy [#a, ai @Double $ zipA (flip (/)) (sumA #a) #a] #d t1


-- * Melt
me1 = melt [#b, #a] #meas #val $ update [ai $ realToFrac @Int64 @Double <$> #b] t1

allEx = do
  print ab1
  print ap2
  print e1
  print ea1
  print ff1
  print ff2
  print fj1
  print m1 
  print me1
  print mf2
  print mg1
  print mg2
  print rb1
  print rn1
  print s1 
  print sb1
  print sb1s
  print v1 
  print v1b
  print za1
  print za2

-- * distinct

dbt1 = distinctByT @Double #a t1
dbt2 = distinctByT @Double #a $ H.reverse t1

-- * Applicative
ap1 = select [ai @[Double] $ sequenceA [#a, #a, #a]] t1



-- * sorting

n1 = 10000000
vec1 = V.enumFromN @Int 0 n1
lt1 = #a <# vec1
  // #b <# V.take n1 (V.concat $ replicate (1 + div n1 (count letters)) letters)
  where letters = V.enumFromTo 'A' 'z'

sort1 = do
  print $ agg [ai $ sumA @Int #a] lt1
  print $ agg [ai $ sumA @Int #a] $ descTD #a lt1
  print $ agg [ai $ sumA @Int #a] $ ascTD #a lt1

-- * empty tables (and therefore polymorphic non-null columns)
et1 = unsafeH $ tc @Void #a [] // tc @Void #b [] // tc @None #c []
et2 = unsafeH $ tc @Int #a [1,2] // tc #b ("as" :: String) // tcF @Int @Maybe #c [Just 1, Nothing]
et3 = unsafeH $ tc @Void #a [] // tc @Void #b2 [] // tc @Void #b [] // tc @None #c3 []
et4 = unsafeH $ tc @Void #a2 [] // tc @Void #b2 [] // tc @None #c3 []

empty1 = unsafeH $ exec (zip3_ #a #b #c) et1 :: Vector (Char, Maybe Int, Maybe String)
empty2 = concatMatchingTables $ et1 :| [et2]
empty3 = t1 `lj` xkey #a et3
empty4 = t1 `ujt` et4


-- * to JSON
slowToJson = either putStrLn (putStrLn . A.encode) $ ensureToJson $ VectorOfRecords t1

fastToJson = either putStrLn (putStrLn . A.encode) $ ensureToJson $ ColumnToVectorMap t1
fastToJson2 = either putStrLn (putStrLn . A.encode) $ ensureToJson $ ColumnToVectorMap et1

rowDictJson = either putStrLn (putStrLn . A.encode) $ ensureToJson $ V.head $ unsafeH $ exec rowDict t1

-- * Typed tables

type Row1 = ["a" := Double, "b" := Int64, "c" := Char, "d" := Text]

tt1 :: IO ()
tt1 = void $ do
  print t1
  tt <- runHinIO $ toTypedTable @Row1 t1
  print tt
  ft <- runHinIO $ filterTypedTable (fmap (> 7) . col #a) tt
  print $ fromTypedTable ft

-- devmain = tt1

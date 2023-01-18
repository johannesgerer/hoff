{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}


module Hoff.Examples where 

import qualified Chronos as C
import           Control.Monad
import           Data.Aeson as A
import           Data.Functor.Compose
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Vector as V
import           Hoff as H
import           Hoff.HQuery.Operators as H
import           Hoff.Serialise
import qualified Prelude
import           System.Environment
import           System.FilePath
import           Text.InterpolatedString.Perl6
import           Yahp hiding (take, group, (^), sum, lookup, error)


error :: ConvertText a [Char] => a -> c
error = Prelude.error . toS

main :: IO ()
main = do
  print keyedTableSerializeRoundTrip
  print t1
  print $ rowsR @('[ "a" ':= Double, "c" ':= Char]) t1
  print t2
  print t4
  ex3
  print sa1
  print m1
  -- mapM putStrLn [show te7, show te4, show te5, show te6, show te8]
  example1
  example2
  ex2
  print 'a'
  example3
  print 'b'
  example4
  print 'c'
  example5
  print 'd'
  example6
  example8
  example7

  crossExample

crossExample = mapM_ print [t1,t2,t3,g1,g2,g3]
  where t1 =   #a <# [1,2,3::Int] //
         #b <# ("abc"::String)
        t2 =   #a <# [10,20,30,40::Int] //
               #c <# ("ABCD"::String)
        t3 =   #a <# tf [10,20,30,40::Int] //
               #b <# (replicate (count $ unsafeH t2) =<< "abc"::String) //
               #c <# tf ("ABCD"::String)
        tf = concat . replicate (count $ unsafeH t1)
        g1 = t1
        g2 = take' 2 t2
        g3 =   #a <# gk [10,20::Int] //
               #b <# gf ("abc"::String) //
               #c <# gk ("AB"::String)
        gf = concat . replicate (count $ unsafeH g2)
        gk = chain $ replicate (count $ unsafeH g1)

  

t1 :: Table
t1 = unsafeH $ #a <# [9,7,7,3 :: Double] // tc #b [10,20,40,30 :: Int64] // tc #c ['a','a','b','b']
  // tc #d ["a","a","b","b"::Text]

t1b :: Table
t1b = unsafeH $ #a <# [9,7,7,3 :: Double] // tc #b [10,20,40,30 :: Int64] // tc #c ['a','a','b','b']
  // tc #d ["a","a","b","asd\211\146"::ByteString]


-- | unsafe serialise
uso :: Serialisable a => a -> EncodeOnlyS a
uso = eitherException . ensureEncodable

t2o :: EoTable
t2o = uso t2

-- t3 = select [ei @Int #b #a, #c </ liftV2 ((+) @Int) #a #b, #k </ fmap (C.Time . fromIntegral @Int64) #b ] t1

t2 = unsafeH $ select [#b </ mapE (id @Int64) #b,  #k </ mapE (\x -> C.Time $ fromIntegral (x :: Int64)) #b ] t1




t4      = unsafeH $ aggBy [#s </ sumA @Double #a] [sn #k #c]                   t1
t4_     = unsafeH $ aggBy [#s </ sumA @Double #a, #t </ co True] [sn #k #c]                   t1
t4'     = unsafeH $ aggBy [ai $ sumA @Double #a] #c                   t1
t4''    = unsafeH $ updateBy [ei #asd $ ca $ sumA @Double #a, ai $ ca $ sumA @Double #a] #c                   t1
f1      = unsafeH $ update [#ab </ fby (sumA @Double #a) #b, #ac </ fby (sumA @Double #a) #c]  t1


t5e= t4 !!?/ (tc' #k ["a"::String])
t5 = t4 !!?/ (tc' #k ['a'])

t6 = t4 !!?/ (unsafeH $ #a <# [1::Int,3, 5] // #k <# ['a','c','b'])

t7 = lj (unsafeH $ #a <# [9,7::Int,7] // #c <# V.fromList "abc") $ xkey ["a","c"] $ unsafeH $ update [ai $ floor_ @Double @Int #a] t1

s = #k <# [1::Int,2]  // #a <# ("ab"::String) // #c <# [1,2::Int] // tcF #d [Nothing, Just (1::Int)]
s2 = #k <# [1::Int,2]  // #a <# ("AB"::String) // #c <# [1,2::Int] // tcF #d [Nothing, Just (1::Int)]

t = #k <# [1::Int,2,3]  // #b <# ("XYZ"::String) // #c <# [10,20,30::Int] //  #d <# [10,20,30::Int]

j1 = lj t $ xkey [#k] s
j2 = lj s $ xkey [#k] t


k1 = xkey ["a","c"] $ update [ai $ floor_ @Double @Int #a] t1


t9 = tcF #a [Just 9,Nothing :: Maybe Int] // tc #b [10,20 :: Int]
  // #b <# [Just True,Nothing] 
  // #s <# [Nothing, Just 'a'] 

exampleJsonArg :: IO ()
exampleJsonArg = void $ do
  pbin <- getEnv "PYTHON_BIN"
  let pp = basicPp (script "s") ()
  snd $ pythonEvalHoffs pbin pp { pArgs = ["arg"] } $ Right []
  snd $ pythonEvalHoffs pbin pp { pArgs = ["arg"], pInput = True } $ Right []
  snd $ pythonEvalHoffs pbin pp { pArgs = ["arg"], pInput = True } $ Right [("t1",uso t1)]
  snd $ pythonEvalHoffs pbin pp { pSource = script "", pArgs = ["arg"], pInput = True } $ Left $ uso t1
  where script :: Text -> Text
        script plural = [qq|
try:
  print(inputCbor)
except:
  print("inputCbor not defined")

print(jsonArg,type(inputDf{plural}),inputDf{plural}, "\\n")
outputDfs=[]
|]

        
example1 :: IO ByteString
example1 = do
  pbin <- getEnv "PYTHON_BIN"
  pythonEvalByteString pbin (basicPp script $ Just "\DLE\NUL") { pArgs = ["arg"] }
  where script :: Text
        script = [q|
data = sys.stdin.buffer.read()
print(sys.argv[1])
with open(pythonEvalByteStringOutputFile, 'wb') as f:
  f.write(('stdin: ' + str(data) + ', raw: ').encode())
  f.write(data)
|]
        
ex2 = do
  pbin <- getEnv "PYTHON_BIN"
  putStrLn @Text "table1"
  print ta
  either error (\t -> printMetas t >> putStrLn (show @Table t)) <=< snd $ pythonEvalHoff
    pbin (originalColNames $ basicPp "outputDf = inputDfs['table2']\nprint(jsonArg)\nprint(outputDf)"
          (True, Nothing @Bool, "jsonNice" :: Text, [1,2,3::Int]))
    $ Right [("table1", t2o),("table2", uso ta)]

-- | round trip
example2 :: IO ()
example2 = do
  pbin <- getEnv "PYTHON_BIN"
  print ta
  print $ meta ta
  t <- either error pure <=< snd $ pythonEvalHoff pbin (originalColNames $ basicPp script ())
    { pArgs = ["arg"] } $ Left $ uso taRound
  print $ meta t
  print $ meta t2
  print t
  print $ t2
  print $ t2 == t
  -- runHinIO $ mergeTablesWith untouchedA untouchedA (\c a b -> bool id (trace c) (a/=b) $ pure a) t t2
  -- -- [c1,c2] <- runHinIO $ mapM (exec @(Maybe ByteString) #bsm) [t,t2]
  -- -- print $ V.zipWith (==) c1 c2
  -- pass
  where t2 = unsafeH $ allToMaybe taRound
        taRound = unsafeH $ H.delete [#bw,#c,#cm] ta -- Word does gets returned as Word64
        script :: Text
        script = [q|
import pandas as pd
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 80)
print(inputDf)
print(inputDf.dtypes)
print(inputDf.iloc[0].map(type))
print("sys.argv", sys.argv, sys.argv[1])
outputDf = inputDf
|]

example3 :: IO Table
example3 = do
  pbin <- getEnv "PYTHON_BIN"
  t <- either error pure <=< snd $ pythonEvalHoff pbin (originalColNames $ basicPp script ())
    { pArgs = ["arg"] } $ Left $ t2o
  t <$ print (meta t)
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDf = pd.DataFrame({'IntCol': [1,10], 'BoolCol':[True,False],
                         'TextCol':['sd','yi'], 'DoubleCol':[1.3, None]})
|]

ta = unsafeH $ tc #a [9,7,3 :: Double]
  // tc #b [10,20,2 :: Int64]
  // tc #b8 [13,20,2 :: Word8]
  // tc #bw [13,20,2 :: Word]
  // tc #b32 [13,20,2 :: Word32]
  // tc #b64 [13,20,2 :: Word64]
  // tc #bi32 [13,20,2 :: Int32]
  // tc #c ['µ','a','b']
  // tc #d ["a","µ","b"::Text]
  // tc #e [True, False, True]
  // tc #f (C.Day <$> [10,20,2])
  // #g <# C.Time . (* 10000000000) <$> [1,3,4]
  // tc #bs (encodeUtf8 <$> ["㎔","µ","b"]) -- ByteString
  // tcF #am (toM [9,7,3 :: Double])
  // tcF #bm (toM [10,20,2 :: Int64])
  // tcF #cm (toM ['µ','a','b'])
  // tcF #dm (toM ["a","µ","b"::Text])
  // tcF #em (toM [True, False, True])
  // tcF #fm (toM (C.Day <$> [10,20,2]))
  // tcF #gm (toM (C.Time . (* 1000000000) <$> [1,3,4]))
  // tcF #bsm (toM $ encodeUtf8 <$> ["㎔","µ","b"]) -- Maybe ByteString
  where toM [a,_,c]=[Just a, Nothing, Just c]
        toM _ = Prelude.error "asd"
        toM :: [a] -> [(Maybe a)]

tw = selectW [] (mapE (=='a') #c) ta
tw' = runWhere (((=='a') <$> #c) ||. ((=='b') <$> #c)) ta
tw''' = H.filter (vec $ toVector [False,False,True,True,True,True,False,False,True]) ta
tw'' = selectW [] (fmap (=='a') #c ||. fmap (=='b') #c) ta

tdel2 = H.deleteW (mapE (=='a') #c) ta
tu = updateW [#a </ co @Double 100] (mapE (=='a') #c) ta
tu3 = updateW [ai $ co @Double 100] (mapE (=='a') #c) ta
tu2 = updateBy [ai $ ca $ sumA @Double #a] [#e] ta

td = H.delete [#a] ta

te      = print $ execRec ANON_F { b = #b :: Exp I Int64 } ta
te2     = print $ flipTypedTable I <$> execRec ANON_F { b = #b :: Exp I Int64 } ta
te3     = unsafeH $ execAggBy (sumA #b) #c ta :: VectorDict Char Int64
te7     = unsafeH $ execAggBy (Just <$> sumA #b) #e ta :: VectorDict Bool (Maybe Int64)
te4     = unsafeH $ execRecAggBy
          ANON_F { a = sumA @Int64 #b, b = mapA (count @(Vector Int64)) #b } (#c :: Exp I Char) ta
te5     = unsafeH $ execRecAggBy
          ANON_F { a = sumA @Int64 #b, b = countA } (co 'a') ta
te6     = unsafeH $ execAggBy (zip_ (sumA @Int64 #b) $ mapA @Int64 count #b) (co 'a') ta
te8     = unsafeH $ execRecAggBy
          ANON_F { a = sumA @Int64 #b, b = countDistinct #e, c = countDistinct #c } (co 'a') ta

idn = #a <# [None ()]

example4 :: IO Table
example4 = do
  pbin <- getEnv "PYTHON_BIN"
  t <- either error pure <=< snd $ pythonEvalHoff pbin (originalColNames $ basicPp script ())
    { pArgs = ["arg"] } $ Left t2o
  print (meta t)
  print t
  let t2 = unsafeH $ select [af @Maybe @Int64 $ hc "IntCol"] t
  print (meta t2)
  return t2
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDf = pd.DataFrame({'IntCol': [None]})
|]


d1 = unsafeH $ dictV [10,2::Int64,3] ("asd" :: String)
td1 = unsafeH $ update [em #lookup $ d1 ?. #b] ta
td2 = unsafeH $ update [ei #lookup $ d1 !. #b] ta

sa1 = unsafeH $ agg [ai $ sumA @Double #a, ai countA, ai $ countDistinct #e, ai $ countDistinct #a] ta

ex3 = do
  pbin <- getEnv "PYTHON_BIN"
  snd $ pythonEvalHoff pbin
    (originalColNames $ basicPp "outputDf = pd.DataFrame({'a': [1.2, None], 'b':['asd','a']})" ())
    $ Right []

eth1 = partitionEithers_ (Proxy @()) #a $ update
       [af $ fmap (bool (Left ()) (Right True) . (> (4::Double))) #a] ta
eth2 = lefts_ (Proxy @()) #a $ update
       [af $ fmap (bool (Left ()) (Right True) . (> (4::Double))) #a] ta
m1 = partitionMaybes #a $ ta


example5 :: IO Table
example5 = do
  pbin <- getEnv "PYTHON_BIN"
  t <- either error pure <=< snd $ pythonEvalHoff pbin (basicPp script ()) { pArgs = ["arg"] } $ Left t2o
  print (meta t)
  print t
  -- let t2 = select [af @Maybe @Int64 "IntCol"] t
  -- print (meta t2)
  return t2
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDf = pd.DataFrame({'Asd': [None, 2.3, 2.2]})
|]


example6 :: IO ()
example6 = do
  pbin <- getEnv "PYTHON_BIN"
  (n,t) <- either error (pure . unzip) <=<
    snd $ pythonEvalHoffs pbin (basicPp script ()) { pArgs = ["arg"] } $ Left t2o
  print n
  mapM_ (print . meta) t
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDfs = [   ('table1', pd.DataFrame({'Asd': [None, 2.3, 2.2]})),
                ('table2', pd.DataFrame({'second table': [None, 2.3, 2.2]}))]
|]


example7 :: IO ()
example7 = do
  pbin <- getEnv "PYTHON_BIN"
  (n,t) <- either error (pure . unzip)
    <=< snd $ pythonEvalHoffs pbin (basicPp script ()) { pArgs = ["arg"] } $ Left t2o
  print n
  mapM_ (\x -> print (meta x) >> print x) t
  where script :: Text
        script = [q|
import pandas as pd
print("sys.argv", sys.argv, sys.argv[1])
outputDfs = [   ('table1', pd.DataFrame({'Asd': []})),
                ('table2', pd.DataFrame({'second table': [None, 2.3, 2.2]}))]
|]


example8 :: IO ()
example8 = do
  pbin <- getEnv "PYTHON_BIN"
  (n,t) <- either error (pure . unzip)
    <=< snd $ pythonEvalHoffs pbin (basicPp script ()) { pArgs = ["arg"] } $ Left t2o
  print n
  mapM_ (\x -> print (meta x) >> print x) t
  where script :: Text
        script = [q|
import pandas as pd
import numpy as np
print("sys.argv", sys.argv, sys.argv[1])
df = pd.DataFrame({'a':[np.NaN,None,"a"],'b':[pd.NA,True,False],'c':[True,True,False],
                        'd':[0.2, np.nan, -1e9], 'f':[0.2, np.nan, -1e9], 'f2':[0.2, 1.1234, -1e9]})
df.b=df.b.astype('boolean')
df.c=df.c.astype('boolean')
df.f=df.f.astype(np.float32)
df.f2=df.f2.astype(np.float32)
print(df.dtypes)


outputDfs = [('a',df)]
# HoffSerialise.dfToHoffCborTemp(outputDfs)
|]


ex9 = execRec ANON_F { a = #a :: Exp I (I Double) } t1


asd = summary $ #a <# [None (), None (), None (), None ()] //
      #b <# ("abbc"::String) //
      tcF #c [Nothing, Just 1, Just 3, Just @Int 2]


asd2 = selectW [] ((== 'a') <$> #asc) t1

asd3 = putStrLn $ unlines $ toList $ showSingleLine @Text  <$> unsafeH (rowDicts t1)

rowVecs = rowVectors @Double $ select [sn #a #a, sn #b #a, sn #c #a] t1

fr1 = fromRows ANON_F { a = fst, b = snd } $ toVector [(1::Int,Just @Word 2), (10, Just 20)]

ct1 = #a <# [1::Int] // tcF #b [Nothing @Int]
ct1p = #a <# [2::Int] // tcF #b [Nothing @Int]

ct2 = unsafeH $ toTypedTable @'["a" := Int, "b" := Maybe Int] ct1
ct2p = unsafeH $ toTypedTable @'["a" := Int, "b" := Maybe Int] ct1p
ct2s = show ct2

cm1 = mconcat [ct2, ct2p]

ct3 = fromTypedTable ct2



devmain = exampleStdout

exampleStdout :: IO ByteString
exampleStdout = do
  pbin <- getEnv "PYTHON_BIN"
  pythonEvalByteString pbin (basicPp script Nothing)
    { pEnv = Just [("PYTHONUNBUFFERED","true")]
    , pStdout = Just $ handleLinesWith $ putStrLn . ("stdout: "<>)
    , pStderr = Just $ handleLinesWith $ putStrLn . ("stderr: "<>)
    }
  where script :: Text
        script = [q|
import time
print("error!", file=sys.stderr)
print("no flush")
time.sleep(1)
print("flush", flush=True)
time.sleep(1)
print("end")
|]

keyedTableSerializeRoundTrip = fmap ((k1 ==) . fromDecodeOnly @KeyedTable . deserialise . serialise)
  $ ensureEncodable k1
  where k1 = unsafeH $ xkey ["a","c"] ta


dups :: HasCallStack => H Table
dups = checkDuplicates True #a #a t1


testCallStack = either putStrLn (\_ -> pass) asd5 :: IO ()

asd5 :: HasCallStack => Either Text (EncodeOnlyJ (VectorOfRecords Table))
asd5 = asd6

asd6 :: HasCallStack => Either Text (EncodeOnlyJ (VectorOfRecords Table))
asd6 = ensureToJson $ VectorOfRecords $ unsafeH $ H.update [ci @(N (IO ())) #asd $ N pass] t1

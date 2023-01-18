{-# LANGUAGE QuasiQuotes #-}
module Hoff.Tests where


import qualified Data.Vector as V
import           Database.SQLite.Simple (open)
import           Hoff as H
import qualified Prelude
import           System.Environment
import           Test.Hspec
import           Test.QuickCheck
import           Yahp hiding (diff)

-- import           Hoff.Asd
-- import           System.TimeIt

-- examples from https://code.kx.com/q/ref/uj/
kxS =   #a <# [1,2::Int] //
        #b <# [2,3::Int] //
        #c <# [5,7::Int]

kxSidx =        #a <# [1,2::Int] //
                #b <# [2,3::Int] //
                #c <# [5,7::Int] //
                #x <# [0,1::Int]

kxT =   #a <# [1,2,3::Int] //
        #b <# [2,3,7::Int] //
        #c <# [10, 20, 30::Int] //
        tc #d ("ABC"::String)

kxSTt = #a <# [1,2,1,2,3::Int] //
        #b <# [2,3,2,3,7::Int] //
        #c <# [5,7,10,20,30::Int] //
        tcF #d [Nothing,Nothing,Just 'A', Just 'B', Just 'C']

kxSTk = #a <# [1,2,3::Int] //
        #b <# [2,3,7::Int] //
        #c <# [10,20,30::Int] //
        tcF #d (fmap Just ("ABC" :: String))

-- custom
u_sa =  #k <# [4,1,2::Int]  //
      tcF #d2 [Just (2::Int), Nothing, Just (1::Int)]
u_t =   #k <# [3,2,1::Int]  //
      tc #d2 [10,20,30::Int]
u_u1 =  #k <# [4,1,2,3::Int]  //
      tcF #d2 [Just 2, Just 30, Just 20, Just (10::Int)]

-- u_sa =  #k <# [4,1,2::Int]  //
--       #a <# ("cab"::String) //
--       #b <# ("cab"::String) //
--       tcF #d1 [Nothing, Nothing, Just (1::Int)] //
--       tcF #d2 [Just (2::Int), Nothing, Just (1::Int)] //
--       tc #d3 [4,1,2::Int] //
--       tc #d4 [5,1,2::Int]
-- u_t =   #k <# [3,2,1::Int]  //
--       #a <# ("ABC"::String) //
--       #c <# [1,2,3::Int] //
--       tcF #d1 [Nothing, Just (10::Int), Just 20] //
--       tc #d2 [10,20,30::Int] //
--       tcF #d3 [Nothing, Just (10::Int), Nothing] //
--       tc #d4 [10,20,30::Int]
-- u_u1 =  #k <# [3,2,1,4::Int]  //
--       #a <# ("ABCc"::String) //
--       tcF #c [Just 1, Just 2, Just (3::Int), Nothing] //
--       tcF #d1 [Nothing, Just (10::Int), Just 20,Nothing] //
--       tcF #d2 [Just 10, Just 20, Just (30::Int), Just 2] //
--       tcF #d3 [Nothing, Just (10::Int), Nothing,Just 4] //
--       tc #d4 [10,20,30::Int, 5] //
--       tcF #b [Nothing, Just 'b', Just 'a', Just 'c']

u_s1 =  #a <# ("aaab"::String) //
      #b <# ("babb"::String) //
      #c <# ("1234"::String)
u_s2 =  #a <# ("qabba"::String) //
      #b <# ("aabbb"::String) //
      #d <# ("12345"::String)
u_s3 =  #a <# ("aabq"::String) //
      #b <# ("baba"::String) //
      tcF #c [Just '1', Just '2', Just '4', Nothing] //
      tcF #d [Just '5', Just '2', Just '3', Just '1']

s =     #k <# [1,2::Int]  //
        #a <# ("ab"::String) //
        #b <# ("ab"::String) //
        tcF #d1 [Nothing, Just (1::Int)] //
        tcF #d2 [Nothing, Just (1::Int)] //
        tc #d3 [1,2::Int] //
        tc #d4 [1,2::Int]

sNone = #k <# [1,2::Int]  //
        #a <# ("ab"::String) //
        #b <# ("ab"::String) //
        tc #d1 [None (), None ()] //
        tc #d5 [None (), None ()] //
        tcF #d2 [Nothing, Just (1::Int)] //
        tc #d3 [1,2::Int] //
        tc #d4 [1,2::Int]

sSNone = #k <# [1,2,1,2::Int]  //
        #a <# ("abab"::String) //
        #b <# ("abab"::String) //
        tcF #d1 [Nothing, Just (1::Int), Nothing, Nothing] //
        tcF #d2 [Nothing, Just (1::Int), Nothing, Just (1::Int)] //
        tc #d3 [1,2,1,2::Int] //
        tc #d4 [1,2,1,2::Int]

t =     #k <# [3,2,1::Int]  //
        #a <# ("ABC"::String) //
        #c <# [1,2,3::Int] //
        tcF #d1 [Nothing, Just (10::Int), Just 20] //
        tc #d2 [10,20,30::Int] //
        tcF #d3 [Nothing, Just (10::Int), Nothing] //
        tc #d4 [10,20,30::Int]


ts =    #k <# [3,2,1::Int]  //
        #a <# ("Aba"::String) //
        #c <# [1,2,3::Int] //
        tcF #d1 [Nothing, Just (1::Int), Nothing] //
        tcF #d2 [Just 10, Just (1::Int), Nothing] //
        tcF #d3 [Nothing, Just (2::Int), Just 1] //
        tc #d4 [10,2,1::Int] //
        tcF #b [Nothing, Just 'b', Just 'a']

tsNone = #k <# [3,2,1::Int]  //
        #a <# ("Aba"::String) //
        #c <# [1,2,3::Int] //
        tcF #d1 [Nothing, Nothing @Int, Nothing] //
        tcF #d2 [Just 10, Just (1::Int), Nothing] //
        tcF #d3 [Nothing, Just (2::Int), Just 1] //
        tc #d4 [10,2,1::Int] //
        tcF #b [Nothing, Just 'b', Just 'a'] //
        tc #d5 (Yahp.replicate 3 $ None ())

st =    #k <# [1,2::Int]  //
        #a <# ("CB"::String) //
        #b <# ("ab"::String) //
        tcF #d1 [Just 20, Just (10::Int)] //
        tcF #d2 [Just 30, Just (20::Int)] //
        tcF #d3 [Nothing, Just (10::Int)] //
        tc #d4 [30, 20::Int] //
        tcF #c [Just 3, Just (2::Int)]

u1 =    #a <# ("ab"::String) //
        #b <# ("ab"::String)

u2 =    #a <# ("ab"::String) //
        #c <# [1,2::Int]

u12 =   #a <# ("abab"::String) //
        tcF #b [Just 'a', Just 'b', Nothing, Nothing] //
        tcF #c [Nothing, Nothing, Just (1::Int), Just 2]

sk      = xkey [#k] s
skNone  = xkey [#k] sNone
tk      = xkey [#k] t

main :: IO ()
main = do
  hspec $ do
    describe "Hoff.Sqlite" $ do
      it "should read in this table" $
        let t1 = allToMaybe $ tcF #column1 [Just @Int64 1, Nothing] //
              tc  #column2 [2 :: Int64 ,3] //
              tcF #column3 [Nothing, Just @Double 2.3] //
              tcF #column4 [Nothing, Just @Text "asd"] //
              tc  #column5 [None (), None ()] //
              tc  #column6 [3,0::Int64] //
              tcF #column7 [Nothing, Just @ByteString "bI"]
            ac = fmap withMeta $ open ":memory:" >>= flip querySqlite_ 
                 "select * from (values(1,2,null,null,null,3,null),(null,3,2.3,\"asd\",null, false, x'6249'))"
        in ac `shouldReturn` withMeta (unsafeH t1)
 
        
    describe "cross" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ let c2 = cross a b
            in shouldBe (withMeta c2) (withMeta c)
          t1 =   #a <# [1,2,3::Int] //
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
        in zipWithM_ cmp [(1::Int)..]
        [(t1, t2, t3)
        ,(g1, g2, g3)
        ]

    describe "partitionMaybes" $ do
      let cmp i (a,b) =  it ("should satisfy case " <> show i) $ shouldBe (meta *** meta $ unsafeH a, a)
                                                                          (meta *** meta $ unsafeH b, b)
          t1 =   tcF #a [Just 1,Nothing @Int] //
                 #c <# [5,7::Int]
          t2 =   #a <# [1::Int] //
                 #c <# [5::Int]
          t3 =   #c <# [7::Int]
        in zipWithM_ cmp [(1::Int)..]
        [(partitionMaybes #a t1, (,) <$> t3 <*> t2)
        ,(partitionMaybes #c t1, (,) <$> H.delete [#c] (take' 0 t1) <*> t1)
        ,(partitionMaybes #d1 sNone, on (,) id <$> H.delete [#d1] sNone
                                     <*> update [vi @Void #d1 $ mempty] (take' 0 sNone))
        ]

    describe "concatMatchingTables" $ do
      let cmp i (a,b) =  it ("should satisfy case " <> show i) $ shouldBe a b
          tn1 = unsafeH $ #a <# [None (), None ()] // #b <# [True, True]
          tn2 = unsafeH $ tcF #a [Just @Int 1, Just 1] // #b <# [False, False]
          tn3 = unsafeH $ #a <# [None ()] // #b <# [False]
          tna = tcF #a [Nothing, Nothing, Just @Int 1, Just 1] // #b <# [True, True, False, False]
          tnb = tcF #a [Just @Int 1, Just 1, Nothing, Nothing, Nothing]
                // #b <# [False, False, True, True, False]
        in zipWithM_ cmp [(1::Int)..]
        [(concatMatchingTables $ tn1 :| [tn2], tna)
        ,(concatMatchingTables $ tn2 :| [tn1, tn3], tnb)
        ]

    describe "Semigroup VectorDict" $ do
      it "should commute with `dict`" $
        property $ \k1' k2' -> let k1 = V.fromList k1'
                                   k2 = V.fromList k2'
                                   k = k1 <> k2 in on (===) unsafeH (dict k k :: VectorDictH Char Char) $ dict k1 k1 <>. dict k2 k2

    describe "distinctVIndices" $ do
      it "should be identical to distinctVIndicesSuperSlow" $ do
        property $ \x y -> let v = t1 (mod 1000 $ getPositive x) $ getPositive y in
          distinctVIndices v === distinctVIndicesSuperSlow v
      it "should be identical to distinctVIndicesSuperSlow @Char" $ do
        property $ \x -> let v = V.fromList x in distinctVIndices @Char v === distinctVIndicesSuperSlow v

    describe "distinctVIndicesHt" $ do
      it "should be identical to distinctVIndicesSuperSlow" $ do
        property $ \x y -> let v = t1 (mod 1000 $ getPositive x) $ getPositive y in
          distinctVIndicesHt v === distinctVIndicesSuperSlow v
      it "should be identical to distinctVIndicesSuperSlow @Char" $ do
        property $ \x -> let v = V.fromList x in distinctVIndicesHt @Char v === distinctVIndicesSuperSlow v

    describe "distinct @Table" $ do
      let cmp i (a,b) =  it ("should satisfy case " <> show i) $ flip shouldBe (distinct' a) b
          s =   #a <# ("aaaabbb"::String) //
                #b <# ("baababb"::String)
          s2 =  #a <# ("aabb"::String) //
                #b <# ("baab"::String)
          k =   #a <# [Just True,Just False,Nothing,Nothing,Just False] //
                #b <# ['b'      ,'b'       ,'a'    ,'a'    ,'b']
          k2 =  #a <# [Just True,Just False,Nothing] //
                #b <# ['b'      ,'b'       ,'a'    ]
        in zipWithM_ cmp [(1::Int)..]
        [(s      ,s2)
        ,(k      ,k2)
        ]

    describe "ujt" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (meta c, c) $ let x = ujt a b  in (meta x, x)
        in zipWithM_ cmp [(1::Int)..]
        [(u1      ,u2     ,u12)
        ,(kxS     ,kxT    ,kxSTt)
        ]

    describe "ij" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (meta c, c) $ let x = ij a b  in (meta x, x)
          t = #s <# ("ifffim"::String) //
              #p <# [7,8,6,6,2,5::Int]
          t2 = #s <# ("ifffim"::String) //
               #p <# [7,8,6,6,2,5::Int] //
               #m <# ("abcdef"::String)
          s = xkey [#s] $       #s <# ("im"::String) //
                                #e <# ("nc"::String) //
                                #m <# [1,2::Int]
          ts =  #s <# ("iim"::String) //
                #p <# [7,2,5::Int] //
                #e <# ("nnc"::String) //
                #m <# [1,1,2::Int]
          ts2 =  #s <# ("iim"::String) //
                #p <# [7,2,5::Int] //
                #m <# [1,1,2::Int] //
                #e <# ("nnc"::String)
        in zipWithM_ cmp [(1::Int)..]
        [(t       ,s     ,ts)
        ,(t2      ,s     ,ts2)
        ]

    describe "ijc" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (meta c, c) $ let x = ijc a b  in (meta x, x)
          t = #s <# ("ifffim"::String) //
              #p <# [7,8,6,6,2,5::Int]
          s = xkey [#s] $       #s <# ("imm"::String) //
                                #e <# ("nca"::String) //
                                #m <# [1,2,3::Int]
          ts =  #s <# ("iimm"::String) //
                #p <# [7,2,5,5::Int] //
                #e <# ("nnca"::String) //
                #m <# [1,1,2,3::Int]
        in zipWithM_ cmp [(1::Int)..]
        [(t       ,s     ,ts)
        ]

    describe "ujmt" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (meta c, c) $ let x = ujmt a b  in (meta x, x)
        in zipWithM_ cmp [(1::Int)..]
        [(s       ,unsafeH $ H.delete [#d5] sNone     ,sSNone)
        ]

    describe "lj" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (meta c, c) $ let x = lj a b  in (meta x, x)
          a1 = #k <# ("aab"::String) //
               #a <# ("XYZ"::String)
          a2 = #k <# ("abc"::String)
          a3 = #k <# ("abc"::String) //
               tcF #a [Just 'X', Just 'Z', Nothing]
        in zipWithM_ cmp [(1::Int)..]
        [(t       ,sk     ,ts)
        ,(t       ,skNone ,tsNone)
        ,(s       ,tk     ,st)
        ,(a2       ,xkey [#k] a1     ,a3)
        -- ,(s       ,xkey [#k] s2    ,s)
        ]

    describe "ljc" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (meta c, c) $ let x = ljc a b  in (meta x, x)
          s =     #k <# [2,1,2::Int]  //
                  #a <# ("cab"::String) //
                  #b <# ("cab"::String) //
                  tcF #d1 [Just 2, Nothing, Just (1::Int)] //
                  tcF #d2 [Just 2, Nothing, Just (1::Int)] //
                  tc #d3 [4, 1,2::Int] //
                  tc #d4 [5, 1,2::Int]

          t =     #k <# [3,2,1::Int]  //
                  #a <# ("ABC"::String) //
                  #c <# [1,2,3::Int] //
                  tcF #d1 [Nothing, Just (10::Int), Just 20] //
                  tc #d2 [10,20,30::Int] //
                  tcF #d3 [Nothing, Just (10::Int), Nothing] //
                  tc #d4 [10,20,30::Int]

          ts =    #k <# [3,2,2,1::Int]  //
                  #a <# ("Acba"::String) //
                  #c <# [1,2,2,3::Int] //
                  tcF #d1 [Nothing, Just 2, Just (1::Int), Nothing] //
                  tcF #d2 [Just 10, Just 2, Just (1::Int), Nothing] //
                  tcF #d3 [Nothing, Just 4, Just (2::Int), Just 1] //
                  tc #d4 [10,5, 2,1::Int] //
                  tcF #b [Nothing, Just 'c', Just 'b', Just 'a']

          s2 =    #k <# [2,1,2::Int]  //
                  #a <# ("cab"::String)

          t2 =    #k <# [1,3,2,1::Int]  //
                  #a <# ("ZABC"::String)

          st2 =   #k <# [2,1,1,2::Int]  //
                  #a <# ("BZCB"::String)


        in zipWithM_ cmp [(1::Int)..] $
        [(t       ,xkey [#k] s,      ts)
        ,let tk = xkey [#k] t in (s       , tk,      lj s tk)
        ,(s2       , xkey [#k] t2,   st2)]
        <>
        let  
          a1 = #k <# ("aab"::String) //
               #a <# ("XYZ"::String)
          a2 = #k <# ("abc"::String)
          a3 = #k <# ("aabc"::String) //
               tcF #a [Just 'X', Just 'Y', Just 'Z', Nothing]
        in [(a2       ,xkey [#k] a1     ,a3)
        ]

    describe "diff" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (a, b, meta c, c)
                           $ let x = diff a b  in (a, b, meta x, x)
        in zipWithM_ cmp [(1::Int)..]
        [let k = xkey [#a,#b] in (kxT, k kxS, take' (-1) kxT)
        ,let k = xkey [#a,#b] in (kxT, k $ take' 0 kxS, kxT)
        ]

    describe "ujk" $ do
      let cmp i (a,b,c) =  it ("should satisfy case " <> show i) $ flip shouldBe (a, b, meta c, c)
                           $ let x = ujk a b  in (a, b, meta x, x)
        in zipWithM_ cmp [(1::Int)..]
        [let k = xkey [#k] in (k u_sa, k u_t, k u_u1)
        ,let k = xkey [#a,#b] in (k u_s1, k u_s2, k u_s3)
        ,let k = xkey [#a,#b] in (k kxS, k kxT, k kxSTk)
        ]

    describe "catMaybes'" $ do
      let cmp i (a,b) =  it ("should satisfy case " <> show @Int i) $ flip shouldBe (a,pure b) (a, catMaybes' Nothing a)
        in do cmp 1 (toWrappedDyn $ toVector [Just 'a', Nothing], V.fromList "a")
              cmp 2 (toWrappedDynI $ toVector [None (), None ()], V.empty @Int)
              let a = toVector [1,2::Int] in cmp 2 (toWrappedDynI a,a)

    describe "catMaybes_" $ do
      let cmp i (a,b) =  it ("should satisfy case " <> show i) $ flip shouldBe (a, pure b) (a, catMaybes_ Nothing a)
        in zipWithM_ cmp [(1::Int)..]
        [(toWrappedDyn $ toVector [Just 'a', Nothing], toWrappedDynI $ V.fromList "a")
        ,let a = toWrappedDynI $ toVector [1,2::Int] in (a,a)
        ]

    describe "grouping should preserve in group row order" $ do
      let cmp i (x,c) =  it ("should satisfy case " <> show i) $ x `shouldBe` c
        in zipWithM_ cmp [(1::Int)..]
        [(show $ V.fromList [0:: Int,1,2,3,4],show $ unsafeH $ execAgg (tableColAgg @Int "i") kxSTt)
        ,(show $ V.fromList [(True, V.fromList [0:: Int,1,2,3,4])]
         ,show $ unsafeH $ zipDict' $ execBy (tableColAgg @Int "i") (co True) kxSTt)
        ]

    describe "virtual column `i`" $ do
      let cmp i (x,c) =  it ("should satisfy case " <> show i) $ (meta x, x) `shouldBe` (meta c, c)
        in zipWithM_ cmp [(1::Int)..]
        [(kxSidx,update [#i] kxS)
        ]

    describe "rename" $ do
      let cmp i (a,b,c,d) =  it ("should satisfy case " <> show i) $ flip shouldBe (a, b, c, d) (a, b, c, rename a b c)
          t1 =   #a <# [1,2::Int] //
                 #c <# [5,7::Int]
          t2 =   #b <# [1,2::Int] //
                 #c <# [5,7::Int]
          t3 =   #c <# [1,2::Int] /|
                 #c <# [5,7::Int]
        in zipWithM_ cmp [(1::Int)..]
        [(#a, #b, t1, t2)
        ,(#a, #c, t1, t3) -- duplicate columns!!! should they be silently created?
        ]

    describe "update" $ do
      let cmp i (a,b) =  it ("should satisfy case " <> show i) $ shouldBe a b
          t1 =   #a <# [1,2::Int] //
                 #c <# [5,7::Int]
          t2 =   #a <# [1,2::Int] //
                 #c <# [5,7::Int] //
                 #d <# [4,4::Int]
        in zipWithM_ cmp [(1::Int)..]
        [(update [ei @Int #d 4] t1, t2)] 

    describe "summary" $ do
      let asd = #a <# [None (), None (), None (), None ()] //
            #b <# ("abbc"::String) //
            tcF #c [Nothing, Just 1, Just 3, Just @Int 2]
          s = "s: 4 rows, 3 cols"
          suma = xkey [s] $ Prelude.foldl1 (//) $ zipWith tc [s, #a, #b, #c] $
                 [["type"::Text,"non nulls","nulls","unique","min","max","most frequent","frequency"]
                 ,["I None","0","4","0","n/a","n/a","n/a","0"]
                 ,["I Char","4","0","3","a","c","b","2"]
                 ,["Maybe Int","3","1","3","1","3","3","1"]]
          cmp i (a,b) = it ("should satisfy case " <> show i) $ shouldBe (a, summary a) (a, b)
        in zipWithM_ cmp [(1::Int)..]
           [(asd, suma)]

    describe "hQuery" $
      let cas i = it ("should satisfy case " <> show i)
          t1 = #a <# [9,7,7,3 :: Double] // tcF #b [Nothing, Nothing,Just 'a', Nothing]
            // tc #c ['a','a','b','b'] // tc #d ["a","a","b","b"::Text]
      in zipWithM_ cas [(1::Int)..]
      [selectBy [ai $ V.filter @Double (> 3.0) <$> #a] #c t1 `shouldBe`
       (xkey #c $ #c <# ("ba" ::String) // #a <# [pure (7::Double), toVector [9,7]])
      ]

  -- print $ length $ concat a
  -- print $ V.sum a
  -- timeIt $ print $ uniqueIndicesAscSuperSlow a
  -- when True $ do
    -- timeIt $ print $ V.head $ uniqueIndicesAsc' a
    -- timeIt $ print $ V.head $ uniqueIndicesAsc a
    -- timeIt $ print $ V.head $ uniqueIndicesAsc'' a
  -- timeIt $ print $ length $ concat a
  -- print $ V.sum a

  -- timeIt $ print $ V.head $ sortV compare a
  -- timeIt $ print $ V.head $ sortVr a


-- t1 n m = ("arsiemiunrfivun" //) . show . flip mod m <$> V.enumFromN (0::Integer) n
t1 n m = (* 10000000) . flip mod m <$> V.enumFromN (0::Int) n
-- a = t1 5000000 200000000000




-- todo: add updateW tests

devmain :: IO ()
devmain = main



devmain2 = withArgs ["--match", "/partitionMaybes/should satisfy case 3/"] main


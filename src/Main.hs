{-# OPTIONS_GHC -fplugin=Data.Record.Anon.Plugin #-}
{-# LANGUAGE NoOverloadedStrings #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE NoRebindableSyntax #-}


import Data.Functor.Identity
import Data.Record.Anon
import Data.Record.Anon.Simple
import Prelude


main :: IO ()
main = do
  print tt1
  print $ get #c1 tt1
  print tt2
  print $ get #c1 tt2
  where tt2 = insert #c3  "c3" tt1
        tt1 = insert #c2  "c2" $ insert #c1  "c1" empty


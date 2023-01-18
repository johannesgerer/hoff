{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}

module Hoff.Examples2 where 

import           Data.SOP
import           Hoff.H
import           Hoff.HQuery.Execution as H
import           Hoff.HQuery.Expressions as H
import           Hoff.Table.Operations
import           Hoff.Table.Types
import qualified Prelude
import           Yahp hiding (take, group, (^), sum, lookup, error)

-- * tables

t1 = unsafeH $ #a <# [9,7,7,3 :: Double] // tc #b [10,20,40,30 :: Int64]
  // tc #c ['a','a','b','b'] // tc #d ["a","a","b","b"::Text]

t1m = allToMaybe t1

-- * simple select

s1 = select [am @Int64 $ next #b, nextD #b,
              am @Int64 $ prev #b, prevD #b, #b, #a] t1

sb1 = selectBy [am @Int64 $ next #b, nextD #b,
                am @Int64 $ prev #b, prevD #b, #b, #a] #a t1

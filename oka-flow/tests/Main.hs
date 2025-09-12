module Main (main) where

import Test.Tasty
import qualified TM.Flow
import qualified TM.Serialization

main :: IO ()
main = defaultMain $ testGroup "oka-flow"
  [ TM.Flow.tests
  , TM.Serialization.tests
  ]

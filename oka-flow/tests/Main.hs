module Main (main) where

import Test.Tasty
import qualified TM.Flow

main :: IO ()
main = defaultMain $ testGroup "oka-flow"
  [ TM.Flow.tests
  ]

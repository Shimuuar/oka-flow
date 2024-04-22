-- |
module Main (main) where

import Test.Tasty
import qualified TM.Meta

main :: IO ()
main = defaultMain $ testGroup "oka-metadata"
  [ TM.Meta.tests
  ]

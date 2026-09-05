-- |
module Main where

import Test.Tasty
import qualified TST.Serialization

main :: IO ()
main = defaultMain $ testGroup "all"
  [ TST.Serialization.tests
  ]

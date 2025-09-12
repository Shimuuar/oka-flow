-- |
{-# OPTIONS_GHC -Wno-orphans #-}
module TM.Serialization
  ( tests
  ) where

import Test.Tasty
import Test.Tasty.QuickCheck

import OKA.Flow.Core.S
import OKA.Flow.Tools

tests :: TestTree
tests = testGroup "Serialization"
  [ testProperty "rountrip args" args_roundtrip
  ]


args_roundtrip :: S FilePath -> Bool
args_roundtrip s
  = Right s == (sexpFromArgs . sexpToArgs) s

----------------------------------------------------------------
-- Orphans
----------------------------------------------------------------

instance Arbitrary (S String) where
  arbitrary = sized $ \case
    n | n <= 1    -> oneof leaves
      | otherwise -> resize (n-1)
                   $ oneof (sexp : leaves)
    where
      leaves = [ pure Nil
               , Atom  <$> atom
               , Param <$> filepath
               ]
      sexp = do n <- choose (0,4)
                S <$> vector n

filepath :: Gen FilePath
filepath = vectorOf 10 (choose ('a', 'z'))

atom :: Gen FilePath
atom = vectorOf 8 (choose ('a', 'z'))

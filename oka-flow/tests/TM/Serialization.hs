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
  [ testProperty "roundtrip S args" args_roundtrip
  , testProperty "roundtrip S JSON" json_roundtrip
  ]


args_roundtrip :: S FilePath -> Bool
args_roundtrip s
  = Right s == (sexpFromArgs . sexpToArgs) s

json_roundtrip :: S Int -> Bool
json_roundtrip s
  = Right s == (sFromJSON . sToJSON) s

----------------------------------------------------------------
-- Orphans
----------------------------------------------------------------

instance Arbitrary1 S where
  liftArbitrary gen = sized $ \case
    n | n <= 1    -> oneof leaves
      | otherwise -> resize (n-1)
                   $ oneof (sexp : leaves)
    where
      leaves = [ pure Nil
               , Atom  <$> atom
               , Param <$> gen
               ]
      sexp = do n <- choose (0,4)
                S <$> vectorOf n (liftArbitrary gen)

instance Arbitrary (S FilePath) where
  arbitrary = liftArbitrary $ vectorOf 10 (choose ('a', 'z'))
instance Arbitrary (S Int) where
  arbitrary = liftArbitrary arbitrary

atom :: Gen FilePath
atom = vectorOf 8 (choose ('a', 'z'))

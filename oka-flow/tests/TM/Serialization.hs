{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
{-# OPTIONS_GHC -Wno-orphans #-}
module TM.Serialization
  ( tests
  ) where

import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Typeable
import Test.Tasty
import Test.Tasty.QuickCheck

import OKA.Flow.Core.S
import OKA.Flow.Core.Types
import OKA.Flow.Tools

tests :: TestTree
tests = testGroup "Serialization"
  [ testProperty "roundtrip S args" args_roundtrip
  , testProperty "roundtrip S JSON" json_roundtrip
  , testJsonRoundtrip @StorePath
  , testJsonRoundtrip @Hash
  ]


args_roundtrip :: S FilePath -> Bool
args_roundtrip s
  = Right s == (sexpFromArgs . sexpToArgs) s

json_roundtrip :: S Int -> Bool
json_roundtrip s
  = Right s == (sFromJSON . sToJSON) s

testJsonRoundtrip :: forall a. (JSON.ToJSON a, JSON.FromJSON a, Arbitrary a, Eq a, Show a, Typeable a) => TestTree
testJsonRoundtrip
  = testProperty ("JSON: " ++ show (typeOf (undefined :: a)))
  $ \(s::a) -> Right s == (JSON.eitherDecode . JSON.encode) s

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

instance Arbitrary StorePath where
  arbitrary = StorePath <$> atom <*> arbitrary
instance Arbitrary Hash where
  arbitrary = Hash . SHA1.hashlazy . BL.pack <$> arbitrary

atom :: Gen FilePath
atom = vectorOf 8 (choose ('a', 'z'))

{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
{-# OPTIONS_GHC -Wno-orphans #-}
module TM.Serialization
  ( tests
  ) where

import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Test.Tasty
import Test.Tasty.QuickCheck
import Data.Typeable
import Unsafe.Coerce

import OKA.Flow.Core.S
import OKA.Flow.Core.Types


tests :: TestTree
tests = testGroup "Serialization"
  [ testGroup "S-roundtrip"
    [ testProperty "roundtrip S args" args_roundtrip
    ]
  , testGroup "JSON"
    [ testJsonRoundtrip @(S Int)
    , testJsonRoundtrip @StorePath
    , testJsonRoundtrip @Hash
    ]
  , testGroup "ToS"
    [ testRoundtripToS @(Result Int)
      -- 
    , testRoundtripToS @()
    , testRoundtripToS @[Result Int]
    , testRoundtripToS @(Maybe (Result Int))
    , testRoundtripToS @(Either (Result Int) (Result Int))
      -- Tuples
    , testRoundtripToS @(Result Int, Result Int)
    , testRoundtripToS @(Result Int, Result Int, Result Int)
    , testRoundtripToS @(Result Int, Result Int, Result Int, Result Int)
    , testRoundtripToS @(Result Int, Result Int, Result Int, Result Int, Result Int)
    , testRoundtripToS @(Result Int, Result Int, Result Int, Result Int, Result Int, Result Int)
    , testRoundtripToS @(Result Int, Result Int, Result Int, Result Int, Result Int, Result Int, Result Int)
    , testRoundtripToS @(Result Int, Result Int, Result Int, Result Int, Result Int, Result Int, Result Int, Result Int)
    ]
  ]


args_roundtrip :: S FilePath -> Bool
args_roundtrip s
  = Right s == (sexpFromArgs . sexpToArgs) s

testJsonRoundtrip :: forall a. (JSON.ToJSON a, JSON.FromJSON a, Arbitrary a, Eq a, Show a, Typeable a) => TestTree
testJsonRoundtrip
  = testProperty ("JSON: " ++ show (typeOf (undefined :: a)))
  $ \(s::a) -> Right s == (JSON.eitherDecode . JSON.encode) s

testRoundtripToS :: forall a. (ToS a, Arbitrary a, Eq a, Show a, Typeable a) => TestTree
testRoundtripToS = testProperty (show (typeOf (undefined :: a)))
  $ \(x :: a) -> fromS (toS x) == Right x


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

-- Dangerous but will work
instance Arbitrary AResult where
  arbitrary = unsafeCoerce (arbitrary @Int)
instance Arbitrary APhony where
  arbitrary = unsafeCoerce (arbitrary @Int)
instance Arbitrary (Result a) where
  arbitrary = unsafeCoerce (arbitrary @Int)
instance Arbitrary (Phony a) where
  arbitrary = unsafeCoerce (arbitrary @Int)


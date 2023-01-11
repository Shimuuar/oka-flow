{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

-- {-# OPTIONS_GHC -Wno-orphans #-}
-- |
module TM.Meta (tests) where

import Data.Typeable
import OKA.Metadata
import Test.Tasty
import Test.Tasty.QuickCheck
import Data.Histogram.Bin
import Data.Histogram.QuickCheck ()

tests :: TestTree
tests = testGroup "Roundtrip"
  [ testSerialise @BinD
  , testSerialise @Int
  , testSerialise @[Int]
  , testSerialise @(Int,Double)
  , testSerialise @(Int,Double,(Int,Int))
  ]

testSerialise :: forall a. (Typeable a, Arbitrary a, Show a, Eq a, IsMeta a) => TestTree
testSerialise
  = testProperty (show (typeOf (undefined :: a)))
  $ \(a::a) -> fromMeta (toMeta a) == a

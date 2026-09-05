{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
module TST.Serialization (tests) where

import Data.Aeson.Types         qualified as JSON
import Data.Typeable
import Test.Tasty
import Test.Tasty.QuickCheck

import OKA.Metadata.Encoding
import OKA.Metadata.Std.Cuts
import TST.Orphanage ()

tests :: TestTree
tests = testGroup "serialization"
  [ testGroup "Meta"
    [ testMeta @(Less      Double)
    , testMeta @(LessEQ    Double)
    , testMeta @(Greater   Double)
    , testMeta @(GreaterEQ Double)
    , testMeta @Range
    ]
  ]


testMeta :: forall a. (MetaEncoding a, Typeable a, Arbitrary a, Show a, Eq a) => TestTree
testMeta
  = testProperty (show (typeOf (undefined :: a)))
  $ \(a :: a) -> fromMeta (metaToJson a) == a

fromMeta :: MetaEncoding a => JSON.Value -> a
fromMeta = either error id . JSON.parseEither parseMeta

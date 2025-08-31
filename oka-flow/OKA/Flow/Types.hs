{-# LANGUAGE UndecidableInstances  #-}
-- |
-- Basic data types used in definition of dataflow program
module OKA.Flow.Types
  ( -- * Workflow primitives
    -- * Store objects API
    ResultSet(..)
  ) where

import Data.Coerce
import GHC.Generics

import OKA.Flow.Core.Types

----------------------------------------------------------------
-- Store objects and interaction
----------------------------------------------------------------

-- | This type class observe isomorphism between tuples and records
--   containing 'StoreObject' and lists of underlying data types
class ResultSet a where
  toResultSet :: a -> [FunID]


instance ResultSet () where
  toResultSet () = []

instance ResultSet (Result a) where
  toResultSet (Result i) = [i]

instance ResultSet a => ResultSet [a] where
  toResultSet    = concatMap toResultSet

deriving via Generically (a,b)
    instance (ResultSet a, ResultSet b) => ResultSet (a,b)
deriving via Generically (a,b,c)
    instance (ResultSet a, ResultSet b, ResultSet c) => ResultSet (a,b,c)
deriving via Generically (a,b,c,d)
    instance (ResultSet a, ResultSet b, ResultSet c, ResultSet d) => ResultSet (a,b,c,d)
deriving via Generically (a,b,c,d,e)
    instance (ResultSet a, ResultSet b, ResultSet c, ResultSet d, ResultSet e) => ResultSet (a,b,c,d,e)
deriving via Generically (a,b,c,d,e,f)
    instance (ResultSet a, ResultSet b, ResultSet c, ResultSet d, ResultSet e, ResultSet f) => ResultSet (a,b,c,d,e,f)



-- | Used for deriving using generics
instance (Generic a, GResultSet (Rep a)) => ResultSet (Generically a) where
  toResultSet (Generically a) = gtoResultSet (from a)

-- Type class for generics
class GResultSet f where
  gtoResultSet :: f () -> [FunID]

deriving newtype instance (GResultSet f) => GResultSet (M1 i c f)

instance (GResultSet f, GResultSet g) => GResultSet (f :*: g) where
  gtoResultSet (f :*: g) = gtoResultSet f <> gtoResultSet g

instance (ResultSet a) => GResultSet (K1 i a) where
  gtoResultSet = coerce (toResultSet @a)

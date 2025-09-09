-- |
-- We need to pass possibly nested data types as parameters to flows.
-- We also need to be able to hash set of parameters. To do so we need
-- to provide standard way to serialize arguments. And easiest way is
-- to use S-expressions
module OKA.Flow.Core.S
  ( -- * S-expression
    S(..)
  , ToS(..)
  , sequenceS
  ) where

import Data.ByteString.Lazy (ByteString)
import Data.Monoid          (Endo(..))

import OKA.Metadata
import OKA.Flow.Core.Result


----------------------------------------------------------------
-- S-expressions
----------------------------------------------------------------

-- | Standard encoding for set of arguments. All parameters to flow
--   are encoded as some S-expression.
data S a
  = Param a
  | Atom  String
  | Nil
  | S     [S a]
  deriving stock (Show,Eq,Read,Functor,Foldable,Traversable)

-- | Some collection of 'Result's
class ToS a where
  -- | Convert value to a S-expression for passing to S.
  toS :: a -> S FunID

-- | Convert S-expression that doesn't contain 'Atom's into list.
sequenceS :: S a -> Maybe [a]
sequenceS = sequence . ($ []) . appEndo . go where
  go = \case
    Param a  -> Endo (Just a :)
    Atom  _  -> Endo (Nothing:)
    Nil      -> mempty
    S     xs -> foldMap go xs



instance ToS (Result a) where
  toS (Result i) = Param i

instance ToS () where
  toS () = Nil

instance (ToS a, ToS b) => ToS (a,b) where
  toS (a,b) = S [toS a, toS b]

instance (ToS a1, ToS a2, ToS a3) => ToS (a1, a2, a3) where
  toS (a1, a2, a3) = S [toS a1, toS a2, toS a3]

instance (ToS a1, ToS a2, ToS a3, ToS a4) => ToS (a1, a2, a3, a4) where
  toS (a1, a2, a3, a4) = S [toS a1, toS a2, toS a3, toS a4]

instance (ToS a1, ToS a2, ToS a3, ToS a4, ToS a5) => ToS (a1, a2, a3, a4, a5) where
  toS (a1, a2, a3, a4, a5) = S [toS a1, toS a2, toS a3, toS a4, toS a5]

instance (ToS a1, ToS a2, ToS a3, ToS a4, ToS a5, ToS a6) => ToS (a1, a2, a3, a4, a5, a6) where
  toS (a1, a2, a3, a4, a5, a6) = S [toS a1, toS a2, toS a3, toS a4, toS a5, toS a6]

instance (ToS a1, ToS a2, ToS a3, ToS a4, ToS a5, ToS a6, ToS a7) => ToS (a1, a2, a3, a4, a5, a6, a7) where
  toS (a1, a2, a3, a4, a5, a6, a7) = S [toS a1, toS a2, toS a3, toS a4, toS a5, toS a6, toS a7]

instance (ToS a1, ToS a2, ToS a3, ToS a4, ToS a5, ToS a6, ToS a7, ToS a8) => ToS (a1, a2, a3, a4, a5, a6, a7, a8) where
  toS (a1, a2, a3, a4, a5, a6, a7, a8) = S [toS a1, toS a2, toS a3, toS a4, toS a5, toS a6, toS a7, toS a8]

instance ToS a => ToS [a] where
  toS = S . fmap toS
instance (ToS a) => ToS (Maybe a) where
  toS Nothing  = Nil
  toS (Just a) = toS a

instance (ToS a, ToS b) => ToS (Either a b) where
  toS (Left  a) = S [Atom "Left",  toS a]
  toS (Right b) = S [Atom "Right", toS b]

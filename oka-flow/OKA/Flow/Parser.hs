{-# LANGUAGE DerivingVia     #-}
{-# LANGUAGE RoleAnnotations #-}
-- |
-- Extremely simple parser for parsing lists
module OKA.Flow.Parser
  ( ListParser(..)
  , runListParser
  , consume
  ) where

import Control.Applicative
-- import Control.Monad
import Control.Monad.Except
import Control.Monad.State.Strict


type role ListParser representational representational

-- | Very simple pure parser which consumes list.
newtype ListParser s a = ListParser
  { get :: [s] -> Either String (a, [s])
  }
  deriving (Functor, Applicative, Alternative, Monad, MonadError String, MonadState [s])
       via StateT [s] (Either String)

-- | Execute parser. It will fail if arguments are not consumed fully
runListParser :: ListParser s a -> [s] -> Either String a
runListParser parser xs =
  parser.get xs >>= \case
    (a, []) -> Right a
    (_, _ ) -> Left "runListParser: unconsumed argument"

-- | Consume single element from the input stream
consume :: ListParser s s
consume = get >>= \case
  []   -> throwError "Not enough inputs"
  s:ss -> s <$ put ss

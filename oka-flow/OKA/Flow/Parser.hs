{-# LANGUAGE RoleAnnotations #-}
-- |
-- Extremely simple parser for parsing lists
module OKA.Flow.Parser
  ( ListParserT(..)
  , ListParser
  , runListParserT
  , runListParser
  , consume
    -- * IO specialization
  , ListParserIO(..)
  , consumeIO
  , runListParserIO
  ) where

import Control.Applicative
-- import Control.Monad
import Control.Monad.Except
import Control.Monad.State.Strict
import Data.Coerce
import Data.Functor.Identity


-- | Very simple pure parser which consumes list.
newtype ListParserT s m a = ListParserT
  { run :: [s] -> m (Either String (a, [s]))
  }
  deriving (Functor, Applicative, Alternative, Monad, MonadIO, MonadError String, MonadState [s])
       via StateT [s] (ExceptT String m)

-- | Pure parser
type ListParser s = ListParserT s Identity


-- | Execute parser. It will fail if arguments are not consumed fully
runListParserT :: Monad m => ListParserT s m a -> [s] -> m (Either String a)
runListParserT parser xs =
  parser.run xs >>= \case
    Left  e       -> pure $ Left e
    Right (a, []) -> pure $ Right a
    Right (_, _ ) -> pure $ Left "runListParser: unconsumed argument"

-- | Execute pure parser.
runListParser :: ListParser s a -> [s] -> Either String a
runListParser p = runIdentity . runListParserT p

-- | Consume single element from the input stream
consume :: Monad m => ListParserT s m s
consume = get >>= \case
  []   -> throwError "Not enough inputs"
  s:ss -> s <$ put ss


----------------------------------------------------------------
--
----------------------------------------------------------------

type role ListParserIO representational representational

-- | Specialization of 'ListParserT' to IO. It is needed to make role
--   annotations representational so that it could be used in deriving
--   via without problems
newtype ListParserIO s a = ListParserIO ([s] -> IO (Either String (a, [s])))
  deriving (Functor, Applicative, Alternative, Monad, MonadIO, MonadError String, MonadState [s])
       via ListParserT s IO

consumeIO :: forall s. ListParserIO s s
consumeIO = coerce (consume @IO @s)

runListParserIO :: forall s a. ListParserIO s a -> [s] -> IO (Either String a)
runListParserIO = coerce (runListParserT @IO @s @a)

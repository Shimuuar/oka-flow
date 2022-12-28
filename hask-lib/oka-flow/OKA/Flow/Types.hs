{-# LANGUAGE DeriveFunctor #-}
-- |
module OKA.Flow.Types where

import Control.Monad
import Control.Monad.STM


----------------------------------------------------------------
-- Helper data types
----------------------------------------------------------------

-- | Bracket function which allows to acquire some resource in
--   concurrent setting and return some value and STM action which
--   releases taken resource.
newtype BracketSTM res a = BracketSTM (res -> STM (STM (), a))
  deriving (Functor)

instance Applicative (BracketSTM res) where
  pure a = BracketSTM $ \_ -> pure (pure (), a)
  (<*>)  = ap

instance Monad (BracketSTM res) where
  BracketSTM br1 >>= f = BracketSTM $ \res -> do
    (release1, a) <- br1 res
    let BracketSTM br2 = f a
    (release2, b) <- br2 res
    pure (release1 *> release2, b)


runBracketSTM :: res -> BracketSTM res a -> STM (STM (), a)
runBracketSTM res (BracketSTM f) = f res

-- | Check that only need to be completed at start of evaluation.
checkAtStart :: STM () -> BracketSTM res ()
checkAtStart chk = BracketSTM $ \_ -> (pure (), ()) <$ chk

-- | Check that only need to be completed at start of evaluation.
checkAtFinish :: STM () -> BracketSTM res ()
checkAtFinish fini = BracketSTM $ \_ -> pure (fini, ())

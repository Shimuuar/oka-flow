{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
module OKA.Flow.Util
  ( -- * Typeable
    typeName
    -- * Concurrency
  , once
  ) where

import Control.Concurrent.MVar
import Data.Typeable

typeName :: forall a. Typeable a => String
typeName = show (typeOf (undefined :: a))


-- | Return action that will be perform action exactly once. Every
--   subsequent call will return cached result
once :: IO a -> IO (IO a)
once action = do
  cache <- newMVar Nothing
  return $ readMVar cache >>= \case
    Just a  -> pure a
    Nothing -> modifyMVar cache $ \case
      Just a  -> return (Just a, a)
      Nothing -> do a <- action
                    return (Just a, a)

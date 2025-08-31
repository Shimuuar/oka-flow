{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
module OKA.Flow.Util
  ( -- * Typeable
    typeName
    -- * Concurrency
  , once
    -- ** Barrier
  , Barrier
  , newBarrier
  , newOpenBarrier
  , checkBarrier
  , clearBarrier
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


-- | Barrier which blocks thread until it's cleared
newtype Barrier = Barrier (MVar ())

newBarrier :: IO Barrier
newBarrier = Barrier <$> newEmptyMVar

newOpenBarrier :: IO Barrier
newOpenBarrier = Barrier <$> newMVar ()
  
checkBarrier :: Barrier -> IO ()
checkBarrier (Barrier mv) = readMVar mv

clearBarrier :: Barrier -> IO ()
clearBarrier (Barrier mv) = () <$ tryPutMVar mv ()


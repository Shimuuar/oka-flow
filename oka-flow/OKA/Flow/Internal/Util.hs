{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
module OKA.Flow.Internal.Util
  ( -- * Typeable
    typeName
    -- * Concurrency
  , once
    -- ** Barrier
  , Barrier
  , newBarrier
  , newOpenBarrier
  , newBarrickadedBarrier
  , checkBarrier
  , clearBarrier
  , failBarrier
  ) where

import Control.Exception
import Control.Concurrent.MVar
import Data.Typeable
import OKA.Flow.Core.Types (DependencyFailed(..))


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
newtype Barrier = Barrier (MVar BarrierState)

data BarrierState
  = BarrierOpen   -- ^ Dependency evaluated OK
  | BarrierFailed -- ^ Dependency failed
  | Barrickaded   -- ^ Dependency shouldn't be evaluated at all

newBarrier :: IO Barrier
newBarrier = Barrier <$> newEmptyMVar

newOpenBarrier :: IO Barrier
newOpenBarrier = Barrier <$> newMVar BarrierOpen

newBarrickadedBarrier :: IO Barrier
newBarrickadedBarrier = Barrier <$> newMVar Barrickaded


checkBarrier :: Barrier -> IO ()
checkBarrier (Barrier mv) = readMVar mv >>= \case
  BarrierOpen   -> pure ()
  BarrierFailed -> throwIO DependencyFailed
  Barrickaded   -> error "INTERNAL error: bad dependency"

clearBarrier :: Barrier -> IO ()
clearBarrier (Barrier mv) = () <$ tryPutMVar mv BarrierOpen

failBarrier :: Barrier -> IO ()
failBarrier (Barrier mv) = () <$ tryPutMVar mv BarrierFailed

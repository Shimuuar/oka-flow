{-# LANGUAGE CPP          #-}
{-# LANGUAGE DataKinds    #-}
{-# LANGUAGE TypeFamilies #-}
-- |
-- Access to PRNG in Flow monad
module OKA.Flow.Eff.PRNG
  ( PrngE
  , PRNG(..)
  , setPrngState
  , runPrngE
  ) where

import Data.Coerce
import Effectful
import Effectful.Dispatch.Static
import System.Random.Stateful

import OKA.Flow.Core.Flow


-- | Ability to use standard splitmix PRNG.
data PrngE :: Effect

type    instance DispatchOf PrngE = Static NoSideEffects
newtype instance StaticRep  PrngE = PrngE StdGen

-- | Handle for using 'StatefulGen' API
data PRNG = PRNG

-- | Handle for PRNG effect
runPrngE
  :: StdGen -- ^ Initial state for a PRNG
  -> Eff (PrngE : es) a
  -> Eff es a
runPrngE g = evalStaticRep (PrngE g)

setPrngState :: (PrngE :> eff) => StdGen -> Flow eff ()
setPrngState g = Flow $ stateStaticRep $ \_ -> ((), PrngE g)

instance (PrngE :> eff) => StatefulGen PRNG (Flow eff) where
  uniformWord32    _ = Flow $ stateStaticRep @PrngE $ coerce (genWord32  @StdGen)
  uniformWord64    _ = Flow $ stateStaticRep @PrngE $ coerce (genWord64  @StdGen)
  uniformWord32R n _ = Flow $ stateStaticRep @PrngE $ coerce (genWord32R @StdGen n)
  uniformWord64R n _ = Flow $ stateStaticRep @PrngE $ coerce (genWord64R @StdGen n)
#if MIN_VERSION_random(1,3,0)
  uniformShortByteString = uniformShortByteStringM
  uniformByteArrayM isPinned n _
    = Flow $ stateStaticRep @PrngE $ coerce (uniformByteArray @StdGen isPinned n)
#else
  uniformShortByteString n _ = Flow $ stateStaticRep @PrngE $ coerce (genShortByteString @StdGen n)
#endif

-- |
module OKA.Flow.Core.Result where


----------------------------------------------------------------
-- Nodes in dataflow graph
----------------------------------------------------------------

-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving stock (Show,Eq,Ord)


-- | Opaque handle to single dataflow in full evaluation graph
newtype AResult = AResult FunID
  deriving stock (Show,Eq,Ord)

-- | Opaque handle to phony dataflow which doesn't produce result.
newtype APhony  = APhony  FunID
  deriving stock (Show,Eq,Ord)


-- | Opaque handle to a dataflow in dataflow graph. It's type tagged
--   in order to provide type safety.
newtype Result a = Result AResult

-- | Opaque handle to result of evaluation of single phony dataflow.
--   This is dataflow which doesn't produce any output.
newtype Phony a = Phony APhony 

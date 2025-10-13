#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "server.h"
#include "macros.h"

class SimpleCommand;
namespace janus
{

  class TxLogServer;
  class RaftServer;
  class RaftServiceImpl : public RaftService
  // usage: RpcHandler(RPC_NAME, N_PARAMS, PARAMS...) { DEFAULTLOGIC }
  {
  public:
    RaftServer *svr_;
    RaftServiceImpl(TxLogServer *sched);

    RpcHandler(RequestVote, 3,
               const ServerProps &, props,
               uint64_t *, voterTerm,
               bool_t *, vote_granted)
    {
      // Cannot use -1 because return type is uint64_t
      *voterTerm = 0;
      *vote_granted = false;
    }

    RpcHandler(AppendEntries, 6,
               const std::vector<Entry> &, entries,
               const uint64_t &, prevLogIndex,
               const uint64_t &, prevLogTerm,
               const ServerProps &, props,
               ServerProps *, followerProps,
               bool_t *, followerAppendOK)
    {
      // Cannot use -1 because return type is uint64_t
      ServerProps dummyProps;
      dummyProps.term = 0;
      dummyProps.serverId = 0;
      dummyProps.lastLogIndex = 0;
      dummyProps.lastLogTerm = 0;
      dummyProps.commitIndex = 0;
      *followerProps = dummyProps;
      *followerAppendOK = false;
    }

    RpcHandler(HelloRpc, 2, const string &, req, string *, res)
    {
      *res = "error";
    };
  };

} // namespace janus

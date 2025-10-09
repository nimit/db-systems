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
  // TODO: These defaults are sensible but the default logic should be handled in receiver functions.
  {
  public:
    RaftServer *svr_;
    RaftServiceImpl(TxLogServer *sched);

    RpcHandler(RequestVote, 3,
               const ServerState &, props,
               uint64_t *, ret1,
               bool_t *, vote_granted)
    {
      // Log_info("RequestVote default value.");
      // Cannot use -1 because return type is uint64_t
      *ret1 = 0;
      *vote_granted = false;
    }

    RpcHandler(EmptyAppendEntries, 1, const ServerState &, props)
    {
    }

    RpcHandler(AppendEntries, 6,
               const MarshallDeputy &, cmd,
               const uint64_t &, index,
               const uint64_t &, term,
               const ServerState &, props,
               uint64_t *, followerTerm,
               bool_t *, followerAppendOK)
    {
      // Log_info("AppendEntries default value.");
      // Cannot use -1 because return type is uint64_t
      *followerTerm = 0;
      *followerAppendOK = false;
    }

    RpcHandler(HelloRpc, 2, const string &, req, string *, res)
    {
      *res = "error";
    };
  };

} // namespace janus

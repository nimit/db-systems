
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "macros.h"

namespace janus
{

  RaftCommo::RaftCommo(PollMgr *poll) : Communicator(poll)
  {
  }
  void RaftCommo::SendEmptyAppendEntries(parid_t par_id, ServerState props)
  {
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first == props.serverId)
      {
        continue; // skip sending to self
      }
      RaftProxy *proxy = (RaftProxy *)p.second;
      // FutureAttr fuattr
      Call_Async(proxy, EmptyAppendEntries, props);
    }
  }

  void RaftCommo::SendRequestVote(parid_t par_id, ServerState props, shared_ptr<QuorumEvent> quorumEvent)
  {
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first == props.serverId)
      {
        continue; // skip self-vote
      }
      // Log_info("[%d] Sending RequestVote to server %d for term %lu", props.serverId, p.first, props.term);
      RaftProxy *proxy = (RaftProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [quorumEvent](Future *fu)
      {
        uint64_t ret1;
        bool_t vote_granted;
        fu->get_reply() >> ret1;
        fu->get_reply() >> vote_granted;
        if (vote_granted)
        {
          quorumEvent->VoteYes();
        }
        else
        {
          quorumEvent->VoteNo();
        }
      };
      Call_Async(proxy, RequestVote, props, fuattr);
    }
  }

  void RaftCommo::SendAppendEntries(parid_t par_id,
                                    siteid_t site_id,
                                    shared_ptr<Marshallable> cmd)
  {
    /*
     * More example code for sending a single RPC to server at site_id
     * You may modify and use this function or just use it as a reference
     */
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first == site_id)
      {
        RaftProxy *proxy = (RaftProxy *)p.second;
        FutureAttr fuattr;
        fuattr.callback = [](Future *fu)
        {
          bool_t followerAppendOK;
          fu->get_reply() >> followerAppendOK;
        };
        /* wrap Marshallable in a MarshallDeputy to send over RPC */
        MarshallDeputy md(cmd);
        Call_Async(proxy, AppendEntries, md, fuattr);
      }
    }
  }

  shared_ptr<IntEvent>
  RaftCommo::SendString(parid_t par_id, siteid_t site_id, const string &msg, string *res)
  {
    auto proxies = rpc_par_proxies_[par_id];
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    for (auto &p : proxies)
    {
      if (p.first == site_id)
      {
        RaftProxy *proxy = (RaftProxy *)p.second;
        FutureAttr fuattr;
        fuattr.callback = [res, ev](Future *fu)
        {
          fu->get_reply() >> *res;
          ev->Set(1);
        };
        /* wrap Marshallable in a MarshallDeputy to send over RPC */
        Call_Async(proxy, HelloRpc, msg, fuattr);
      }
    }
    return ev;
  }

} // namespace janus

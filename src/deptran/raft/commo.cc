
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
  void RaftCommo::SendEmptyAppendEntries(parid_t par_id, siteid_t site_id, ServerState props)
  {
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first != site_id)
      {
        continue;
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

  void RaftCommo::SendAppendEntries(parid_t par_id, siteid_t site_id, shared_ptr<Marshallable> cmd, uint64_t index, uint64_t term, ServerState props,
                                    std::recursive_mutex *mtx, std::vector<uint64_t> *nextIndex, std::vector<uint64_t> *matchIndex)
  {
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first == site_id)
      {
        RaftProxy *proxy = (RaftProxy *)p.second;
        FutureAttr fuattr;
        fuattr.callback = [site_id, index, props, nextIndex, matchIndex, mtx](Future *fu)
        {
          uint64_t followerTerm;
          bool_t followerAppendOK;
          fu->get_reply() >> followerTerm;
          fu->get_reply() >> followerAppendOK;

          if (followerTerm > props.term)
          {
            // TODO: Step down as leader
            Log_info("[SAE] Discovered higher term %lu from follower %d", props.serverId, followerTerm, site_id);
            return;
          }
          std::lock_guard<std::recursive_mutex> lock(*mtx);
          if (followerAppendOK)
          {
            Log_info("[SAE] Appended entry at index %lu for follower %d", props.serverId, index, site_id);
            (*nextIndex)[site_id] = (*nextIndex)[site_id] + 1;
            (*matchIndex)[site_id] = index;
          }
          else
          {
            Log_info("[SAE] Failed to append entry at index %lu for follower %d", props.serverId, index, site_id);
            (*nextIndex)[site_id] = (*nextIndex)[site_id] - 1;
            (*matchIndex)[site_id] = index;
          }
          std::lock_guard<std::recursive_mutex> unlock(*mtx);
        };
        /* wrap Marshallable in a MarshallDeputy to send over RPC */
        MarshallDeputy md(cmd);
        Call_Async(proxy, AppendEntries, md, index, term, props, fuattr);
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

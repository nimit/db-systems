
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
      // Log_debug("[%d] Sending RequestVote to server %d for term %lu", props.serverId, p.first, props.term);
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
                                    std::recursive_mutex *mtx, int *state, std::vector<uint64_t> *nextIndex, std::vector<uint64_t> *matchIndex)
  {
    std::lock_guard<std::recursive_mutex> lock(*mtx);
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first != site_id)
      {
        continue;
      }
      RaftProxy *proxy = (RaftProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [site_id, index, props, state, nextIndex, matchIndex, mtx](Future *fu)
      {
        uint64_t followerTerm;
        bool_t followerAppendOK;
        fu->get_reply() >> followerTerm;
        fu->get_reply() >> followerAppendOK;

        mtx->lock();
        if (followerTerm > props.term)
        {
          Log_info("[SAE] Discovered higher term from follower %d (%lu > %lu)", site_id, followerTerm, props.term);
          // TODO (optimization): Find a better way to step down?
          *state = 0; // TODO (TEST): STEP DOWN TO FOLLOWER
          return;
        }
        if (followerAppendOK)
        {
          Log_info("[SAE] Appended entry at index %lu for follower %d", index, site_id);
          (*nextIndex)[site_id] = (*nextIndex)[site_id] + 1;
          (*matchIndex)[site_id] = index;
        }
        else
        {
          Log_info("[SAE] Failed to append entry at index %lu for follower %d", index, site_id);
          auto lastIndex = (*matchIndex)[site_id];
          auto newIndex = (*nextIndex)[site_id] - 1;
          (*nextIndex)[site_id] = newIndex <= lastIndex ? lastIndex + 1 : newIndex;
          (*matchIndex)[site_id] = index;
        }
        mtx->unlock();
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      MarshallDeputy md(cmd);
      Call_Async(proxy, AppendEntries, md, index, term, props, fuattr);
      break;
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

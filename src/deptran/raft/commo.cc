
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
  void RaftCommo::SendEmptyAppendEntries(parid_t par_id, siteid_t site_id, ServerProps props, std::recursive_mutex *mtx, int *state, int *term)
  {
    auto proxies = rpc_par_proxies_[par_id];
    for (auto &p : proxies)
    {
      if (p.first != site_id)
      {
        continue;
      }
      RaftProxy *proxy = (RaftProxy *)p.second;
      FutureAttr fuattr;
      fuattr.callback = [site_id, props, mtx, state, term](Future *fu)
      {
        uint64_t followerTerm;
        bool_t followerReceivedHeartbeat;
        fu->get_reply() >> followerTerm;
        fu->get_reply() >> followerReceivedHeartbeat;
        if (followerTerm > props.term)
        {
          Log_info("[SEAE] Discovered higher term from follower %d (%lu > %lu)", site_id, followerTerm, props.term);
          // TODO: STEP DOWN & UPDATE TERM
          std::lock_guard<std::recursive_mutex> lock(*mtx);
          *state = 0;
          *term = followerTerm;
        }
      };
      Call_Async(proxy, EmptyAppendEntries, props);
    }
  }

  void RaftCommo::SendRequestVote(parid_t par_id, ServerProps props, shared_ptr<QuorumEvent> quorumEvent)
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

  void RaftCommo::SendAppendEntries(parid_t par_id, siteid_t site_id, vector<Entry> entries, uint64_t prevLogIndex, uint64_t prevLogTerm, ServerProps props,
                                    std::recursive_mutex *mtx, int *state, int *leaderTerm, std::vector<uint64_t> *nextIndex, std::vector<uint64_t> *matchIndex)
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
      fuattr.callback = [site_id, prevLogIndex, props, mtx, state, leaderTerm, nextIndex, matchIndex](Future *fu)
      {
        ServerProps followerProps;
        bool_t followerAppendOK;
        fu->get_reply() >> followerProps;
        fu->get_reply() >> followerAppendOK;

        std::lock_guard<std::recursive_mutex> lock(*mtx);
        if (followerProps.term > props.term)
        {
          Log_info("[SAE] LEADER %d discovered higher term from follower %d (%lu > %lu)", props.serverId, site_id, followerProps.term, props.term);
          // TODO (optimization): Find a better way to step down?
          // TODO (TEST): STEP DOWN TO FOLLOWER
          *state = 0;
          *leaderTerm = followerProps.term;
          return;
        }
        if (followerAppendOK)
        {
          Log_info("[SAE] Appended entries from index %lu to %lu for follower %d", prevLogIndex, followerProps.lastLogIndex, site_id);
          (*nextIndex)[site_id] = followerProps.lastLogIndex + 1;
          (*matchIndex)[site_id] = followerProps.lastLogIndex;
        }
        else
        {
          // Log_info("[SAE] Failed to append entries from index %lu for follower %d. Will try from index %lu", prevLogIndex, site_id, followerProps.lastLogIndex + 1);
          (*nextIndex)[site_id] = followerProps.lastLogIndex + 1;
          // Keep matchIndex as it is.... (can also be reset to 0, doesn't seem to matter)
          // (*matchIndex)[site_id] = 0;
        }
      };
      Call_Async(proxy, AppendEntries, entries, prevLogIndex, prevLogTerm, props, fuattr);
      return;
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

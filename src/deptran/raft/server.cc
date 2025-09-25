

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"

namespace janus
{

  RaftServer::RaftServer(Frame *frame)
  {
    frame_ = frame;
    /* Your code here for server initialization. Note that this function is
       called in a different OS thread. Be careful about thread safety if
       you want to initialize variables here. */

    // Every ELECTION_TIMEOUT microseconds, check when last heartbeat was received
    // If last heartbeat was received more than ELECTION_TIMEOUT microseconds ago, start election
    // (have to still add randomization because ELECTION_TIMEOUT is constant for all servers)

    Log_info("[%d] Starting RaftServer at time %lu", loc_id_, GetTime());
  }

  RaftServer::~RaftServer()
  {
    Log_info("[%d] Destroying RaftServer", loc_id_);
    serverShutdown = true; // will close down all coroutines (checks inside the loops - should not cause problems in other parts of the codej)
  }

  void RaftServer::Setup()
  {
    /* Your code here for server setup. Due to the asynchronous nature of the
    framework, this function could be called after a RPC handler is triggered.
    Your code should be aware of that. This function is always called in the
    same OS thread as the RPC handlers. */

    // Only coroutine that doesn't stop (no return/yield) until server is destroyed
    rrr::Coroutine::CreateRun(
        [this]()
        {
          Log_info("[%d] Started coroutine at time %lu", loc_id_, GetTime());
          while (true)
          {
            if (serverShutdown)
            {
              return;
            }
            // TODO: Randomize timeout here
            auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(ELECTION_TIMEOUT);
            timeout->Wait();
            if (state != FOLLOWER)
            {
              continue;
            }
            uint64_t total_us = GetTime();
            if (total_us - lastHeartbeatTime > ELECTION_TIMEOUT)
            {
              Log_info("[%d] Election timeout reached, starting election", loc_id_);
              startElection();
            }
          }
        });
  }

  void RaftServer::startElection()
  {
    state = CANDIDATE;
    currentTerm += 1;
    votedFor = loc_id_;
    // Effectively resetting the election timer
    lastHeartbeatTime = GetTime();
    rrr::Coroutine::CreateRun(
        [this]()
        {
          ServerState props = GetServerState();
          int totalServers = commo()->rpc_par_proxies_[partition_id_].size();
          // quorum is totalServers/2 instead of totalServers/2 + 1 because we skip self-vote
          int quorum = totalServers / 2;
          Log_info("[%d] Starting election for term %lu at time %lu (total: %d, quorum: %d)", loc_id_, currentTerm, GetTime(), totalServers, quorum);
          Log_info("[%d] Props: term %lu, lastLogIndex %lu, lastLogTerm %lu", loc_id_, props.term, props.lastLogIndex, props.lastLogTerm);
          auto quorumTimeout = Reactor::CreateSpEvent<TimeoutEvent>(2e6); // 2s
          shared_ptr<QuorumEvent> quorumEvent = Reactor::CreateSpEvent<QuorumEvent>(totalServers, quorum);
          auto sendRequestVoteTime = GetTime();
          commo()->SendRequestVote(partition_id_, props, quorumEvent);
          while (!quorumEvent->IsReady() && !quorumTimeout->IsReady() && !serverShutdown)
          {
            // So that it doesn't busy-wait
            auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(20000); // 20ms
            timeout->Wait();
            if (lastHeartbeatTime > sendRequestVoteTime)
            {
              break;
            }
            Log_info("[%d] Waiting for votes... (have %d yes, %d no)", loc_id_, quorumEvent->n_voted_yes_, quorumEvent->n_voted_no_);
          }
          if (quorumTimeout->IsReady() || serverShutdown)
          {
            Log_info("[%d] Election coroutine timeout for term %lu at time %lu", loc_id_, currentTerm, GetTime());
            return;
          }
          else if (lastHeartbeatTime > sendRequestVoteTime)
          {
            Log_info("[%d] Election aborted. Received heartbeat at %lu vs sent request vote at %lu", loc_id_, lastHeartbeatTime, sendRequestVoteTime);
            return;
          }
          Log_info("[%d] Election finished for term %lu at time %lu result - yes: %d, no: %d", loc_id_, currentTerm, GetTime(), quorumEvent->Yes(), quorumEvent->No());
          if (quorumEvent->Yes())
          {
            InitiateLeader();
          }
        });
    // TODO: HANDLE
    // If receive AppendEntries from new leader with term >= currentTerm, step down to follower
  }

  void RaftServer::InitiateLeader()
  {
    Log_info("[%d] Becoming leader for term %lu at time %lu", loc_id_, currentTerm, GetTime());
    votedFor = -1;
    state = LEADER;
    rrr::Coroutine::CreateRun(
        [this]()
        {
          while (state == LEADER && !serverShutdown)
          {
            auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(HEARTBEAT_INTERVAL);
            commo()->SendEmptyAppendEntries(partition_id_, GetServerState());
            Log_info("[%d] Sent heartbeats at time %lu", loc_id_, GetTime());
            timeout->Wait();
          }
        });
  }

  ServerState RaftServer::GetServerState()
  {
    ServerState props;
    props.term = currentTerm;
    props.serverId = loc_id_;
    props.lastLogIndex = log.size(); // index of last log entry (starts from 1 according to the raft paper)
    props.lastLogTerm = log.empty() ? 0 : log.back().first;
    return props;
  }

  void RaftServer::ReceiveHeartbeat(ServerState *props)
  {
    if (props->term < currentTerm)
    {
      return;
    }
    lastHeartbeatTime = GetTime();
    currentTerm = props->term;
    state = FOLLOWER;
    votedFor = -1;
    // Log_info("[%d] Received heartbeat from server %d for term %lu at time %lu", loc_id_, props->serverId, props->term, lastHeartbeatTime);
  }

  bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                         uint64_t *index,
                         uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    // *index = 0;
    // *term = 0;

    // Return false if this server is not the leader
    // If server is the leader, append to new log entry
    return state == LEADER;
  }

  void RaftServer::GetState(bool *is_leader, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    Log_info("[%d] GetState called at time %lu | state: %d, term: %lu", loc_id_, GetTime(), state, currentTerm);
    *is_leader = state == LEADER;
    *term = currentTerm;
  }

  std::pair<uint64_t, bool> RaftServer::AskVote(ServerState *props)
  {
    // Log_info("[%d] Received RequestVote from server %d for term %lu at time %lu", loc_id_, props->serverId, props->term, GetTime());
    if (votedFor == props->serverId)
      return {currentTerm, true};
    if (votedFor != -1 || props->term <= currentTerm)
    {
      return {currentTerm, false};
    }
    // Check if log is up to date
    // Log_info("[%d] Checking if log is up to date", loc_id_);
    bool logEmpty = log.empty();

    bool greaterTerm = logEmpty || (props->lastLogTerm > log.back().first);
    bool equalTerm = logEmpty || (props->lastLogTerm == log.back().first);
    bool greaterIndex = logEmpty || (props->lastLogIndex >= log.size());

    if (greaterTerm || (equalTerm && greaterIndex))
    {
      // Log_info("[%d] Logs are up to date, voting yes", loc_id_);
      votedFor = props->serverId;
      return {currentTerm, true};
    }

    // TODO: Step down if leader and candidate's term is higher (but don't vote yes if log isn't up to date)
    // OR BETTER OPTION? When running for elections, dry run and see if server gets elected. Increase the currentTerm after election (but send currentTerm + 1 in RequestVote RPC)
    return {currentTerm, false};
  }

  void RaftServer::SyncRpcExample()
  {
    /* This is an example of synchronous RPC using coroutine; feel free to
       modify this function to dispatch/receive your own messages.
       You can refer to the other function examples in commo.h/cc on how
       to send/recv a Marshallable object over RPC. */
    Coroutine::CreateRun([this]()
                         {
    string res;
    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     0, "hello", &res);
    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->status_ == Event::TIMEOUT) {
      Log_info("[%d] timeout happens", loc_id_);
    } else {
      Log_info("[%d] rpc response is: %s", loc_id_, res.c_str()); 
    } });
  }

  /* Do not modify any code below here */

  void RaftServer::Disconnect(const bool disconnect)
  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    verify(disconnected_ != disconnect);
    // global map of rpc_par_proxies_ values accessed by partition then by site
    static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
    if (_proxies.find(partition_id_) == _proxies.end())
    {
      _proxies[partition_id_] = {};
    }
    RaftCommo *c = (RaftCommo *)commo();
    if (disconnect)
    {
      verify(_proxies[partition_id_][loc_id_].size() == 0);
      verify(c->rpc_par_proxies_.size() > 0);
      auto sz = c->rpc_par_proxies_.size();
      _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
      c->rpc_par_proxies_ = {};
      verify(_proxies[partition_id_][loc_id_].size() == sz);
      verify(c->rpc_par_proxies_.size() == 0);
    }
    else
    {
      verify(_proxies[partition_id_][loc_id_].size() > 0);
      auto sz = _proxies[partition_id_][loc_id_].size();
      c->rpc_par_proxies_ = {};
      c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
      _proxies[partition_id_][loc_id_] = {};
      verify(_proxies[partition_id_][loc_id_].size() == 0);
      verify(c->rpc_par_proxies_.size() == sz);
    }
    disconnected_ = disconnect;
  }

  bool RaftServer::IsDisconnected()
  {
    return disconnected_;
  }

} // namespace janus

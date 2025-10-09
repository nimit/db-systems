

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
          // Log_info("[%d] Started coroutine at time %lu", loc_id_, GetTime());
          while (true)
          {
            if (serverShutdown)
            {
              return;
            }
            int randomDelay = GetRandomDelayMS(50); // up to 50ms
            auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(ELECTION_TIMEOUT + randomDelay);
            timeout->Wait();
            if (state != FOLLOWER)
            {
              continue;
            }
            StartElection();
          }
        });
  }

  void RaftServer::StartElection()
  {
    uint64_t total_us = GetTime();
    if (total_us - lastHeartbeatTime <= ELECTION_TIMEOUT)
    {
      return;
    }
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    Log_info("[%d] (%s) starting election", loc_id_, IsDisconnected() ? "disconnected" : "connected");
    // OR BETTER OPTION? When running for elections, dry run and see if server gets elected. Increase the currentTerm after election (but send currentTerm + 1 in RequestVote RPC)
    state = CANDIDATE;
    currentTerm += 1;
    votedFor = loc_id_;
    // Effectively resetting the election timer
    lastHeartbeatTime = GetTime();
    rrr::Coroutine::CreateRun(
        [this]()
        {
          std::lock_guard<std::recursive_mutex> lock(mtx_);
          ServerState props = GetServerState();
          int totalServers = commo()->rpc_par_proxies_[partition_id_].size() - 1;
          if (totalServers <= 0)
          {
            Log_info("[%d] Not enough servers (%d) to start election for term %lu at time %lu", loc_id_, totalServers + 1, currentTerm, GetTime());
            return;
          }
          // quorum is totalServers/2 instead of totalServers/2 + 1 because we skip self-vote
          int quorum = totalServers / 2;
          Log_info("[%d] Starting election for term %lu at time %lu (total: %d, quorum: %d)", loc_id_, currentTerm, GetTime(), totalServers, quorum);
          Log_debug("[%d] Props: term %lu, lastLogIndex %lu, lastLogTerm %lu", loc_id_, props.term, props.lastLogIndex, props.lastLogTerm);
          auto quorumTimeout = Reactor::CreateSpEvent<TimeoutEvent>(1e6); // 1s
          shared_ptr<QuorumEvent> quorumEvent = Reactor::CreateSpEvent<QuorumEvent>(totalServers, quorum);
          auto sendRequestVoteTime = GetTime();
          commo()->SendRequestVote(partition_id_, props, quorumEvent);
          while (!quorumEvent->IsReady() && !quorumTimeout->IsReady() && !serverShutdown)
          {
            // So that it doesn't busy-wait
            auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(2e4); // 20ms
            timeout->Wait();
            if (lastHeartbeatTime > sendRequestVoteTime)
            {
              break;
            }
            Log_info("[%d] Waiting for votes... (have %d yes, %d no)", loc_id_, quorumEvent->n_voted_yes_, quorumEvent->n_voted_no_);
          }
          if (quorumTimeout->IsReady() || serverShutdown)
          {
            Log_info("[%d] Election coroutine timeout/shutdown for term %lu at time %lu", loc_id_, currentTerm, GetTime());
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
          else
          {
            Reactor::CreateSpEvent<TimeoutEvent>(GetRandomDelayMS(100))->Wait();
            StartElection();
            return;
          }
        });
    // TODO: HANDLE
    // If receive AppendEntries from new leader with term >= currentTerm, step down to follower
  }

  void RaftServer::InitiateLeader()
  {
    Log_info("[%d] Becoming leader for term %lu at time %lu", loc_id_, currentTerm, GetTime());

    Log_debug("[%d] InitiateLeader locking mtx", loc_id_);
    mtx_.lock();
    std::vector<janus::SiteProxyPair> proxies = commo()->rpc_par_proxies_[partition_id_];
    votedFor = -1;
    state = LEADER;
    nextIndex = std::vector<uint64_t>(proxies.size(), log.size() + 1);
    matchIndex = std::vector<uint64_t>(proxies.size(), 0);
    mtx_.unlock();
    Log_info("[%d] InitiateLeader unlocked mtx", loc_id_);

    rrr::Coroutine::CreateRun(
        [this]()
        {
          while (state == LEADER && !serverShutdown)
          {
            auto props = GetServerState();
            int matchServers = 0;
            matchIndex[loc_id_] = log.size();
            // we don't care about nextIndex because it is skipped (when sending entries, leader skips self)

            std::lock_guard<std::recursive_mutex> lock(mtx_);
            auto proxies = commo()->rpc_par_proxies_[partition_id_];
            for (auto &m : matchIndex)
            {
              if (m > commitIndex)
              {
                matchServers += 1;
              }
            }
            if (matchServers > (proxies.size() / 2) && commitIndex < log.size())
            {
              commitIndex += 1;
              auto entry = log[commitIndex - 1];
              app_next_(*entry.second);
              lastApplied += 1;
              Log_info("[%d] (LEADER) | Updated commitIndex to %lu at time %lu", loc_id_, commitIndex, GetTime());
            }

            auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(HEARTBEAT_INTERVAL);
            for (auto &p : proxies)
            {
              if (p.first == loc_id_)
              {
                continue; // skip sending to self
              }
              // TODO: TEST STEP DOWN
              if (props.lastLogIndex >= nextIndex[p.first])
              {
                // Log_debug("[%d] (LEADER) | will send appendEntries... %lu >= %lu", loc_id_, props.lastLogIndex, nextIndex[p.first]);
                auto logIndex = nextIndex[p.first];
                auto entry = log.at(logIndex - 1);
                auto logTerm = entry.first;
                auto cmd = entry.second;
                // Log_debug("[%d] (LEADER) | Sent AppendEntries to server %d for logIndex %lu at time %lu", loc_id_, p.first, logIndex, GetTime());
                commo()->SendAppendEntries(partition_id_, p.first, cmd, logIndex, logTerm, props, &mtx_, (int *)&state, &nextIndex, &matchIndex);
              }
              else
              {
                commo()->SendEmptyAppendEntries(partition_id_, p.first, GetServerState());
              }
            }
            // Log_debug("[%d] (LEADER) | Sent heartbeats at time %lu", loc_id_, GetTime());
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
    props.leaderCommit = commitIndex;
    return props;
  }

  /// This method should be called on every incoming request. Rules according to Raft paper section 5.1
  /// Should return whether to continue serving the request or not
  bool RaftServer::Verify(ServerState *props)
  {
    // Log_debug("[%d] Verify called", loc_id_);
    if (props->term > currentTerm)
    {
      // Higher term request received. Step down to follower if leader/candidate
      // Decide what to do in the respective handler functions
      // votedFor is reset when the new candidate's term is higher than the current term because it is guaranteed that we did not vote for a candidate in that term
      std::lock_guard<std::recursive_mutex> lock(mtx_);
      currentTerm = props->term;
      votedFor = -1;
      state = FOLLOWER;
      return true;
    }
    else if (props->term == currentTerm)
    {
      return true;
    }
    Log_info("[%d] Rejected request from %d in Verify (%d < %d)", loc_id_, props->serverId, props->term, currentTerm);
    return false;
  }

  // ReceviveHeartbeat is called when a server receives a heartbeat from another server
  // It is different from Verify only because it sets lastHeartbeatTime. We don't want to set lastHeartbeatTime in AskVote because the server initiating the request is not the leader
  void RaftServer::ReceiveHeartbeat(ServerState *props)
  {
    // Log_debug("[%d] ReceiveHeartbeat called", loc_id_);
    if (!Verify(props))
    {
      return;
    }
    lastHeartbeatTime = GetTime();
    //* Already happened in Verify
    // currentTerm = props->term;
    // state = FOLLOWER;
    // votedFor = -1;
    if (props->leaderCommit > commitIndex)
    {
      // Log_debug("[%d] RE: Want to update commitIndex & lastApplied to %lu from %lu (lastLogIndex: %lu)", loc_id_, props->leaderCommit, commitIndex, log.size());
      commitIndex = std::min(props->leaderCommit, (uint64_t)log.size());
      // Apply all entries between lastApplied and commitIndex
      for (uint64_t i = lastApplied; i < commitIndex; i++)
      {
        auto entry = log[i];
        // Log_info("[%d] RE: Applying log entry at index %lu for term %lu at time %lu", loc_id_, i + 1, entry.first, GetTime());
        app_next_(*entry.second);
        lastApplied += 1;
      }
    }
    // Log_debug("[%d] Received heartbeat from server %d for term %lu at time %lu", loc_id_, props->serverId, props->term, lastHeartbeatTime);
  }

  // TODO (optimization): receive multiple entries at once
  /*
    term is the log entry's term
    index is the log entry's index (starts from 1)
    Right now only sends a single entry (easier to implement and debug)
  */
  pair<uint64_t, bool> RaftServer::ReceiveEntry(shared_ptr<Marshallable> &cmd, uint64_t index, uint64_t term, ServerState *props)
  {
    // Log_debug("[%d] ReceiveEntry called", loc_id_);
    if (!Verify(props))
    {
      Log_info("[%d] RE: Received entry from stale term (%lu < %lu)", loc_id_, props->term, currentTerm);
      return {currentTerm, false};
    }
    ReceiveHeartbeat(props);

    auto currentProps = GetServerState();
    if (currentProps.lastLogIndex == index && currentProps.lastLogTerm == term)
    {
      // handle case where the term and log index match (already appended entry)
      return {currentTerm, true};
    }
    else if (currentProps.lastLogIndex >= index && currentProps.lastLogTerm != term)
    {
      // If conflicting entry (same index, different term), delete that entry and all that follow it, then append new entry
      log.resize(index - 1);
      currentProps = GetServerState();
      if (currentProps.lastLogTerm != props->lastLogTerm)
      {
        return {currentTerm, false};
      }
      log.push_back({term, cmd});
      Log_info("[%d] RE: Fixed log inconsistency and appended new log entry at index %lu for term %lu at time %lu", loc_id_, log.size(), props->term, GetTime());
      return {currentTerm, true};
    }
    else if (currentProps.lastLogIndex == index - 1)
    {
      log.push_back({term, cmd});
      Log_info("[%d] RE: Appended new log entry at index %lu for term %lu at time %lu", loc_id_, currentProps.lastLogIndex + 1, props->term, GetTime());
      return {currentTerm, true};
    }
    // Reject entry if log isn't up to date with leader
    return {currentTerm, false};
  }

  bool RaftServer::Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    // *index = 0;
    // *term = 0;

    // Return false if this server is not the leader
    // If server is the leader, append to new log entry
    if (state != LEADER)
    {
      return false;
    }
    log.push_back({currentTerm, cmd});
    Log_info("[%d] (LEADER_START) | Appending new log entry at index %lu for term %lu at time %lu", loc_id_, log.size(), currentTerm, GetTime());
    *index = log.size();
    *term = currentTerm;
    while (lastApplied != *index)
    {
      // busy-wait
      Reactor::CreateSpEvent<TimeoutEvent>(1e5)->Wait();
    }
    return true;
  }

  void RaftServer::GetState(bool *is_leader, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    Log_debug("[%d] (%s) GetState called at time %lu | state: %d, term: %lu", loc_id_, IsDisconnected() ? "disconnected" : "connected", GetTime(), state, currentTerm);
    *is_leader = state == LEADER;
    *term = currentTerm;
  }

  std::pair<uint64_t, bool> RaftServer::AskVote(ServerState *props)
  {
    // Log_debug("[%d] Received RequestVote from server %d for term %lu at time %lu", loc_id_, props->serverId, props->term, GetTime());
    if (!Verify(props))
    {
      return {currentTerm, false};
    }
    if (votedFor == props->serverId)
    {
      return {currentTerm, true};
    }
    if (votedFor != -1 || props->term < currentTerm)
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
      lastHeartbeatTime = GetTime();
      return {currentTerm, true};
    }
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
      Log_debug("[%d] COMMO: %p", loc_id_, c);
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

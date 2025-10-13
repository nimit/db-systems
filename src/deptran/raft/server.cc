

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

    // Only coroutine that doesn't stop (no return) until server is destroyed
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
            // Log_debug("[%d] Election timeout complete (will start election if server is follower)", loc_id_, currentTerm, GetTime());
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
    // Log_debug("[%d] REMOVE: In StartElection", loc_id_);
    uint64_t total_us = GetTime();
    if (total_us - lastHeartbeatTime <= ELECTION_TIMEOUT)
    {
      return;
    }
    // Log_debug("[%d] (%s) starting election", loc_id_, IsDisconnected() ? "disconnected" : "connected");
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    // OR BETTER OPTION? When running for elections, dry run and see if server gets elected. Increase the currentTerm after election (but send currentTerm + 1 in RequestVote RPC)
    state = CANDIDATE;
    currentTerm += 1;
    votedFor = loc_id_;
    rrr::Coroutine::CreateRun(
        [this]()
        {
          ServerProps props = GetServerProps();
          // quorum is NSERVERS/2 instead of NSERVERS/2 + 1 because we skip self-vote
          int quorum = int(NSERVERS / 2);
          Log_info("[%d] Starting election for term %lu at time %lu (total: %d, quorum: %d)", loc_id_, currentTerm, GetTime(), NSERVERS, quorum);
          Log_debug("[%d] Props: term %lu, lastLogIndex %lu, lastLogTerm %lu", loc_id_, props.term, props.lastLogIndex, props.lastLogTerm);
          auto quorumTimeout = Reactor::CreateSpEvent<TimeoutEvent>(ELECTION_TIMEOUT); // 1s
          shared_ptr<QuorumEvent> quorumEvent = Reactor::CreateSpEvent<QuorumEvent>(NSERVERS, quorum);
          // Effectively reset the election timer
          lastHeartbeatTime = GetTime();
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
            // Log_debug("[%d] Waiting for votes... (have %d yes, %d no)", loc_id_, quorumEvent->n_voted_yes_, quorumEvent->n_voted_no_);
          }
          if (quorumTimeout->IsReady() || serverShutdown)
          {
            Log_info("[%d] Election coroutine timeout/shutdown for term %lu at time %lu", loc_id_, currentTerm, GetTime());
            Reactor::CreateSpEvent<TimeoutEvent>(GetRandomDelayMS(500))->Wait();
            StartElection();
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
            Reactor::CreateSpEvent<TimeoutEvent>(GetRandomDelayMS(500))->Wait();
            StartElection();
            return;
          }
        });
  }

  void RaftServer::InitiateLeader()
  {
    Log_info("[%d] Becoming leader for term %lu at time %lu", loc_id_, currentTerm, GetTime());
    mtx_.lock();
    std::vector<janus::SiteProxyPair> proxies = commo()->rpc_par_proxies_[partition_id_];
    votedFor = -1;
    state = LEADER;
    nextIndex = std::vector<uint64_t>(NSERVERS, std::max((uint64_t)1, log.size()));
    matchIndex = std::vector<uint64_t>(NSERVERS, 0);
    mtx_.unlock();
    Log_debug("[%d] InitiateLeader unlocked mtx", loc_id_);

    rrr::Coroutine::CreateRun(
        [this]()
        {
          while (state == LEADER && !serverShutdown)
          {
            mtx_.lock();
            auto props = GetServerProps();
            matchIndex[loc_id_] = props.lastLogIndex;
            // we don't care about nextIndex[loc_id_] because it is skipped (when sending entries, leader skips self)
            auto matchCopy = matchIndex;
            // majoorityIdx is n/2 (because vector idx will start from 0)
            int majorityIdx = int(NSERVERS / 2);
            std::nth_element(matchCopy.begin(), matchCopy.begin() + majorityIdx, matchCopy.end());
            int highestReplicatedMajority = matchCopy[majorityIdx];
            while (highestReplicatedMajority > commitIndex)
            {
              auto entry = log[highestReplicatedMajority - 1];
              if (entry.first != currentTerm)
              {
                highestReplicatedMajority -= 1;
                continue;
              }
              while (commitIndex < highestReplicatedMajority)
              {
                lastApplied += 1;
                commitIndex += 1;
                app_next_(*log[commitIndex - 1].second);
              }
              Log_info("[%d] (LEADER) | Updated commitIndex to %lu at time %lu", loc_id_, commitIndex, GetTime());
              break;
            }

            auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(HEARTBEAT_INTERVAL);
            auto proxies = commo()->rpc_par_proxies_[partition_id_];
            for (auto &p : proxies)
            {
              if (p.first == loc_id_)
              {
                continue; // skip sending to self
              }
              if (props.lastLogIndex >= nextIndex[p.first])
              {
                // Log_debug("[%d] (LEADER) | will send appendEntries... %lu >= %lu", loc_id_, props.lastLogIndex, nextIndex[p.first]);
                auto prevLogIndex = nextIndex[p.first] - 1;
                auto prevLogTerm = prevLogIndex == 0 ? 0 : log.at(prevLogIndex - 1).first;
                vector<Entry> entries;
                for (size_t i = prevLogIndex; i < log.size(); ++i)
                {
                  Entry e;
                  e.term = log[i].first;
                  e.cmd = MarshallDeputy(log[i].second);
                  entries.push_back(std::move(e));
                }
                // Log_debug("[%d] (LEADER) | Sent AppendEntries to server %d for logIndex %lu at time %lu", loc_id_, p.first, logIndex, GetTime());
                commo()->SendAppendEntries(partition_id_, p.first, entries, prevLogIndex, prevLogTerm, props, &mtx_, (int *)&state, (int *)&currentTerm, &nextIndex, &matchIndex);
              }
              else
              {
                commo()->SendEmptyAppendEntries(partition_id_, p.first, GetServerProps(), &mtx_, (int *)&state, (int *)&currentTerm);
              }
            }
            mtx_.unlock();
            // Log_debug("[%d] (LEADER) | Sent heartbeats at time %lu", loc_id_, GetTime());
            timeout->Wait();
          }
        });
  }

  ServerProps RaftServer::GetServerProps()
  {
    ServerProps props;
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    props.term = currentTerm;
    props.serverId = loc_id_;
    props.lastLogIndex = log.size(); // index of last log entry (starts from 1 according to the raft paper)
    props.lastLogTerm = log.empty() ? 0 : log.back().first;
    props.commitIndex = commitIndex;
    return props;
  }

  /// This method should be called on every incoming request. Rules according to Raft paper section 5.1
  /// Should return whether to continue serving the request or not
  bool RaftServer::Verify(ServerProps *props)
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
  void RaftServer::ReceiveHeartbeat(ServerProps *props)
  {
    // Log_debug("[%d] Received heartbeat from server %d for term %lu at time %lu", loc_id_, props->serverId, props->term, GetTime());
    if (!Verify(props))
    {
      return;
    }
    lastHeartbeatTime = GetTime();
    //* Already happened in Verify
    // currentTerm = props->term;
    // state = FOLLOWER;
    // votedFor = -1;

    if (props->commitIndex > commitIndex)
    {
      // Log_debug("[%d] RE: Want to update commitIndex & lastApplied to %lu from %lu (lastLogIndex: %lu)", loc_id_, props->leaderCommit, commitIndex, log.size());
      auto currentProps = GetServerProps();
      if (currentProps.lastLogTerm != props->term)
      {
        // Cannot commit another leader's log
        return;
      }
      commitIndex = std::min(props->commitIndex, currentProps.lastLogIndex);
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

  /*
    term is the log entry's term
    index is the log entry's index (starts from 1)
    Right now only sends a single entry (easier to implement and debug)
  */
  pair<ServerProps, bool> RaftServer::ReceiveEntry(vector<ReceivedEntry> entries, uint64_t prevLogIndex, uint64_t prevLogTerm, ServerProps *props)
  {
    Log_debug("[%d] ReceiveEntry called w %d entries from %d for term %lu (leader: %d)", loc_id_, entries.size(), prevLogIndex, prevLogTerm, props->serverId);
    if (!Verify(props))
    {
      Log_info("[%d] RE: Received entry from stale term (%lu < %lu)", loc_id_, props->term, currentTerm);
      return {GetServerProps(), false};
    }
    ReceiveHeartbeat(props);

    auto currentProps = GetServerProps();
    if (currentProps.lastLogIndex < prevLogIndex)
    {
      // follower log not up to date, try sending entry that matches server's index + 1
      Log_debug("[%d] RE: Follower log not up to date, trying to send entry that matches index %lu", loc_id_, currentProps.lastLogIndex);
      return {currentProps, false};
    }
    else if (currentProps.lastLogIndex >= prevLogIndex && prevLogIndex != 0 && log.at(prevLogIndex - 1).first != prevLogTerm)
    {
      // TODO (optimization): Find last consistient index w/ server (serverLastLogIndex - entries.size() vs currentProps.lastLogIndex)

      // If conflicting entry (same index, different term), delete that entry and all that follow it, then append new entry
      log.resize(prevLogIndex - 1);
      currentProps = GetServerProps();
      if (currentProps.lastLogTerm != prevLogTerm)
      {
        currentProps.lastLogIndex = std::max((uint64_t)1, currentProps.lastLogIndex) - 1; // So we can send the conflicting entry again (at index = prevLogIndex)
        Log_debug("[%d] RE: Removed logs until index %lu (term mismatch %d != %d), retrying with index %lu for term %lu", loc_id_, prevLogIndex, currentProps.lastLogTerm, prevLogTerm, currentProps.lastLogIndex + 1, props->term);
        return {currentProps, false};
      }
      Log_debug("[%d] RE: Fixed log inconsistency for term %lu at time %lu", loc_id_, currentProps.lastLogIndex, props->term, GetTime());
    }

    if (currentProps.lastLogIndex == prevLogIndex)
    {
      for (const auto &e : entries)
      {
        log.push_back({std::move(e.term), std::move(e.cmd)});
      }
      Log_info("[%d] RE: Appended %d new log entries from index %lu for term %lu at time %lu", loc_id_, entries.size(), prevLogIndex, props->term, GetTime());
    }

    // Logs up to date
    // TODO(optimization): Add ReceiveHeartbeat content here (and remove SendEmptyAppendEntries RPC)
    return {currentProps, true};
  }

  bool RaftServer::Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    // *index = 0;
    // *term = 0;

    // Return false if this server is not the leader
    // If server is the leader, append to new log entry
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    if (state != LEADER)
    {
      return false;
    }
    log.push_back({currentTerm, cmd});
    Log_info("[%d] (LEADER_START) | Appending new log entry at index %lu for term %lu at time %lu", loc_id_, log.size(), currentTerm, GetTime());
    *index = log.size();
    *term = currentTerm;
    // wait for leader coroutine to send AppendEntries
    // auto timeout = Reactor::CreateSpEvent<TimeoutEvent>(HEARTBEAT_INTERVAL);
    // timeout->Wait();
    return true;
  }

  void RaftServer::GetState(bool *is_leader, uint64_t *term)
  {
    /* Your code here. This function can be called from another OS thread. */
    Log_debug("[%d] (%s) GetState called at time %lu | state: %d, term: %lu", loc_id_, IsDisconnected() ? "disconnected" : "connected", GetTime(), state, currentTerm);
    *is_leader = state == LEADER;
    *term = currentTerm;
  }

  std::pair<uint64_t, bool> RaftServer::AskVote(ServerProps *props)
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

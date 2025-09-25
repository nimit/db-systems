#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "commo.h"

namespace janus
{

#define HEARTBEAT_INTERVAL 100000
#define ELECTION_TIMEOUT 200000

  enum STATE
  {
    FOLLOWER,
    CANDIDATE,
    LEADER
  };

  class RaftServer : public TxLogServer
  {
  public:
    std::pair<uint64_t, bool> AskVote(ServerState *props);
    void ReceiveHeartbeat(ServerState *props);

  private:
    uint64_t lastHeartbeatTime = 0;
    STATE state = FOLLOWER;
    bool serverShutdown = false;
    //
    int votedFor = -1;
    uint64_t currentTerm = 0;
    uint64_t lastApplied = 0;
    uint64_t commitIndex = 0;
    std::vector<uint64_t> nextIndex;
    std::vector<uint64_t> matchIndex;
    std::vector<pair<uint64_t, shared_ptr<Marshallable>>> log;
    //
    ServerState GetServerState();
    void InitiateLeader();
    inline uint64_t GetTime()
    {
      struct timespec curr_time;
      clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
      int64_t total_us = static_cast<int64_t>(curr_time.tv_sec) * 1000000LL + curr_time.tv_nsec / 1000;
      return total_us;
    }

    /* do not modify this class below here */

  public:
    RaftServer(Frame *frame);
    ~RaftServer();

    bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
    void GetState(bool *is_leader, uint64_t *term);

  private:
    bool disconnected_ = false;
    void Setup();
    void startElection();

  public:
    void SyncRpcExample();
    void Disconnect(const bool disconnect = true);
    void Reconnect()
    {
      Disconnect(false);
    }
    bool IsDisconnected();

    virtual bool HandleConflicts(Tx &dtxn,
                                 innid_t inn_id,
                                 vector<string> &conflicts)
    {
      verify(0);
    };
    RaftCommo *commo()
    {
      return (RaftCommo *)commo_;
    }
  };
} // namespace janus

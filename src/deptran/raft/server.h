#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "commo.h"

namespace janus
{

#define HEARTBEAT_INTERVAL 100000

  class RaftServer : public TxLogServer
  {
  public:
    /* Your data here */
    bool askVote(ServerState *props);

    /* Your functions here */

  private:
    int votedFor = -1;
    uint64_t currentTerm = 0;
    uint64_t lastApplied = 0;
    uint64_t commitIndex = 0;
    std::vector<uint64_t> nextIndex;
    std::vector<uint64_t> matchIndex;
    std::vector<pair<uint64_t, shared_ptr<Marshallable>>> log;

    /* do not modify this class below here */

  public:
    RaftServer(Frame *frame);
    ~RaftServer();

    bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
    void GetState(bool *is_leader, uint64_t *term);

  private:
    bool disconnected_ = false;
    void Setup();

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

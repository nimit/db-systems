#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"
#include "raft_rpc.h"

namespace janus
{
  class TxData;

  class RaftCommo : public Communicator
  {

  public:
    RaftCommo() = delete;
    RaftCommo(PollMgr *);

    void SendRequestVote(parid_t par_id, ServerProps props, shared_ptr<QuorumEvent> quorumEvent);
    void SendEmptyAppendEntries(parid_t par_id, siteid_t site_id, ServerProps props, std::recursive_mutex *mtx, int *state, int *term);
    void SendAppendEntries(parid_t par_id, siteid_t site_id, shared_ptr<Marshallable> cmd, uint64_t index, uint64_t term, ServerProps props,
                           std::recursive_mutex *mtx, int *state, int *leaderTerm, std::vector<uint64_t> *nextIndex, std::vector<uint64_t> *matchIndex);

    shared_ptr<IntEvent> SendString(parid_t par_id, siteid_t site_id, const string &msg, string *res);

    /* Do not modify this class below here */

    friend class FpgaRaftProxy;

  public:
#ifdef RAFT_TEST_CORO
    std::recursive_mutex rpc_mtx_ = {};
    uint64_t rpc_count_ = 0;
#endif
  };

} // namespace janus

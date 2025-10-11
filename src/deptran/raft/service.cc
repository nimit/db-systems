
#include "../marshallable.h"
#include "service.h"
#include "server.h"

namespace janus
{

  RaftServiceImpl::RaftServiceImpl(TxLogServer *sched)
      : svr_((RaftServer *)sched)
  {
    struct timespec curr_time;
    clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
    srand(curr_time.tv_nsec);
  }

  void RaftServiceImpl::HandleRequestVote(const ServerProps &props, uint64_t *voterTerm, bool_t *vote_granted, rrr::DeferredReply *defer)
  {
    std::pair<uint64_t, bool> res = svr_->AskVote((ServerProps *)&props);
    *voterTerm = res.first;
    *vote_granted = res.second;
    Log_info("[%d] Voting %d for server %d for term %lu", svr_->loc_id_, *vote_granted, props.serverId, props.term);
    defer->reply();
  }

  void RaftServiceImpl::HandleEmptyAppendEntries(const ServerProps &props, rrr::DeferredReply *defer)
  {
    svr_->ReceiveHeartbeat((ServerProps *)&props);
    defer->reply();
  }

  void RaftServiceImpl::HandleAppendEntries(const std::vector<Entry> &entries, const uint64_t &prevLogIndex, const uint64_t &prevLogTerm, const ServerProps &props, ServerProps *followerProps, bool_t *followerAppendOK, rrr::DeferredReply *defer)
  {
    std::vector<ReceivedEntry> receivedEntries;
    receivedEntries.reserve(entries.size());

    for (const auto &e : entries)
    {
      ReceivedEntry re;
      re.cmd = const_cast<MarshallDeputy &>(e.cmd).sp_data_;
      re.term = e.term;

      receivedEntries.push_back(std::move(re));
    }
    auto result = svr_->ReceiveEntry(receivedEntries, prevLogIndex, prevLogTerm, (ServerProps *)&props);
    *followerProps = result.first;
    *followerAppendOK = result.second;
    defer->reply();
  }

  void RaftServiceImpl::HandleHelloRpc(const string &req, string *res, rrr::DeferredReply *defer)
  {
    /* Your code here */
    Log_info("receive an rpc: %s", req.c_str());
    *res = "world";
    defer->reply();
  }

} // namespace janus;

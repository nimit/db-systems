
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

  void RaftServiceImpl::HandleRequestVote(const ServerProps &props, uint64_t *ret1, bool_t *vote_granted, rrr::DeferredReply *defer)
  {
    /* Your code here */
    // *ret1 = 0;
    // *vote_granted = false;
    std::pair<uint64_t, bool> res = svr_->AskVote((ServerProps *)&props);
    *ret1 = res.first;
    *vote_granted = res.second;
    Log_info("[%d] Voting %d for server %d for term %lu", svr_->loc_id_, *vote_granted, props.serverId, props.term);
    defer->reply();
  }

  void RaftServiceImpl::HandleEmptyAppendEntries(const ServerProps &props, rrr::DeferredReply *defer)
  {
    svr_->ReceiveHeartbeat((ServerProps *)&props);
    defer->reply();
  }

  void RaftServiceImpl::HandleAppendEntries(const MarshallDeputy &md_cmd, const uint64_t &index, const uint64_t &term, const ServerProps &props, uint64_t *followerTerm, bool_t *followerAppendOK, rrr::DeferredReply *defer)
  {
    /* Your code here */
    std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy &>(md_cmd).sp_data_;
    auto result = svr_->ReceiveEntry(cmd, index, term, (ServerProps *)&props);
    *followerTerm = result.first;
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

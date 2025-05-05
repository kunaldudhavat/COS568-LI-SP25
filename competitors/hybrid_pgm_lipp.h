// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/**
 * HybridPGMLipp<…> :
 *  - ingest into a small DPGM model,
 *  - once buffer reaches threshold, swap it out to a flush‐buffer,
 *    reset the model, and background‐merge into LIPP::BulkMerge().
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
 public:
  explicit HybridPGMLipp(const std::vector<int>& params)
    : dp_(params),
      li_(params),
      flush_threshold_( params.empty() ? 500000 : params[0] ),
      shutting_down_(false)
  {
    flusher_ = std::thread(&HybridPGMLipp::flushLoop, this);
  }

virtual  ~HybridPGMLipp()  {
    {
      std::lock_guard<std::mutex> lk(mu_);
      shutting_down_ = true;
      cv_.notify_one();
    }
    flusher_.join();
  }

  // Bulk‐load initial data directly into LIPP
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) 
  {
    return li_.Build(data, num_threads);
  }

  // point‐lookup: try DPGM first, then LIPP
  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const 
  {
    auto v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) return v;
    return li_.EqualityLookup(key, thread_id);
  }

  // range‐sum across both
  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const 
  {
    return dp_.RangeQuery(lo, hi, thread_id)
         + li_.RangeQuery(lo, hi, thread_id);
  }

  // insert into DPGM, trigger flush when threshold reached
  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id) 
  {
    dp_.Insert(kv, thread_id);

    std::lock_guard<std::mutex> lk(mu_);
    active_buf_.push_back(kv);
    if (active_buf_.size() >= flush_threshold_) {
      // swap out to flush‐buf, reset DPGM, and wake flusher
      active_buf_.swap(flush_buf_);
      dp_.Reset();
      cv_.notify_one();
    }
  }

  std::string name() const  { return "HybridPGMLipp"; }

  // approximate total memory footprint
  std::size_t size() const  {
    return dp_.size() + li_.size();
  }

  bool applicable(bool unique, bool rq, bool ins, bool mt,
                  const std::string&) const  {
    return unique && !mt;
  }

 private:
  void flushLoop() {
    std::vector<KeyValue<KeyType>> batch;
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&]{ return shutting_down_ || !flush_buf_.empty(); });
        if (shutting_down_ && flush_buf_.empty()) break;
        flush_buf_.swap(batch);
      }
      // merge entire batch in one go
      li_.BulkMerge(batch);
      batch.clear();
    }
  }

  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                              li_;

  // double‐buffer for flushed entries
  std::vector<KeyValue<KeyType>> active_buf_, flush_buf_;
  const size_t                    flush_threshold_;

  // flusher coordination
  std::mutex                     mu_;
  std::condition_variable        cv_;
  bool                           shutting_down_;
  std::thread                    flusher_;
};
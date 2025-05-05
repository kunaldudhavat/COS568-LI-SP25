// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/**
 * HybridPGMLipp<Key,SearchClass,PGMError> :
 *  - ingest into a small DynamicPGM model,
 *  - once buffer reaches flush_threshold, swap it out,
 *    reset the model, and background‐merge into LIPP via BulkMerge().
 */
template <typename KeyType, typename SearchClass, size_t PGMError = 16>
class HybridPGMLipp : public Base<KeyType> {
public:
  explicit HybridPGMLipp(const std::vector<int>& params)
    : _flush_threshold(params.empty() ? 500000u : size_t(params[0])),
      dp_(params),
      li_(params),
      shutting_down_(false)
  {
    flusher_ = std::thread(&HybridPGMLipp::flushLoop, this);
  }

  ~HybridPGMLipp() override {
    {
      std::lock_guard<std::mutex> lk(mu_);
      shutting_down_ = true;
      cv_.notify_one();
    }
    flusher_.join();
  }

  // Bulk‐load initial data directly into LIPP
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) override
  {
    return li_.Build(data, num_threads);
  }

  // point‐lookup: try DynamicPGM first, then LIPP
  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const override
  {
    auto v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) return v;
    return li_.EqualityLookup(key, thread_id);
  }

  // range‐sum across both
  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const override
  {
    return dp_.RangeQuery(lo, hi, thread_id)
         + li_.RangeQuery(lo, hi, thread_id);
  }

  // insert into DynamicPGM, trigger flush when threshold reached
  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id) override
  {
    dp_.Insert(kv, thread_id);
    {
      std::lock_guard<std::mutex> lk(mu_);
      flush_buf_.push_back(kv);
      if (flush_buf_.size() >= _flush_threshold) {
        // swap out buffer, reset dp_, wake background flusher
        active_buf_.swap(flush_buf_);
        dp_.Reset();
        cv_.notify_one();
      }
    }
  }

  std::string name() const override { return "HybridPGMLipp"; }

  // approximate total memory footprint
  std::size_t size() const override {
    return dp_.size() + li_.size();
  }

  bool applicable(bool unique, bool rq, bool ins,
                  bool mt, const std::string&) const override
  {
    return unique && !mt;  // only single‐thread, unique keys
  }

  // used to tag in CSV: PGM‐error & flush threshold
  std::vector<std::string> variants() const override {
    return {
      SearchClass::name(),
      std::to_string(PGMError),
      std::to_string(_flush_threshold)
    };
  }

private:
  void flushLoop() {
    std::vector<KeyValue<KeyType>> batch;
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&]{ return shutting_down_ || !active_buf_.empty(); });
        if (shutting_down_ && active_buf_.empty()) break;
        batch.swap(active_buf_);
      }
      li_.BulkMerge(batch);
      batch.clear();
    }
  }

  const size_t                               _flush_threshold;
  DynamicPGM<KeyType, SearchClass, PGMError> dp_;
  Lipp<KeyType>                              li_;

  // double‐buffer for flushed entries
  std::vector<KeyValue<KeyType>>             active_buf_, flush_buf_;

  // flusher coordination
  std::mutex                                 mu_;
  std::condition_variable                    cv_;
  bool                                       shutting_down_;
  std::thread                                flusher_;
};
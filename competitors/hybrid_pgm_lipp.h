// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "../util.h"
#include "../utils/bloom_filter.hpp"   // ← your BloomFilter implementation
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/**
 * HybridPGMLipp<…> :
 *  - ingest into a small DPGM model + Bloom,
 *  - once buffer reaches threshold, swap it out, RESET both DPGM+Bloom,
 *    and background‐merge into LIPP::BulkMerge().
 *  - on lookup: if Bloom says “no”, go straight to LIPP; otherwise probe DPGM then LIPP.
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
public:
  explicit HybridPGMLipp(const std::vector<int>& params)
    : dp_(params),
      li_(params),
      flush_threshold_( params.empty() ? 500000 : params[0] ),
      bloom_( /* expected items */ flush_threshold_*2,
              /* false‐positive rate */ 0.01 ),
      shutting_down_(false)
  {
    flusher_ = std::thread(&HybridPGMLipp::flushLoop, this);
  }

  ~HybridPGMLipp() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      shutting_down_ = true;
      cv_.notify_one();
    }
    flusher_.join();
  }

  // bulk‐load INITIAL dataset into LIPP; do NOT touch bloom here
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads)
  {
    return li_.Build(data, num_threads);
  }

  // point‐lookup: bloom → (DPGM if maybe) → LIPP
  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const
  {
    // 1) if bloom says “definitely not in our tiny buffer” skip DPGM
    if (!bloom_.contains(key)) {
      // static or missing keys go straight into LIPP
      return li_.EqualityLookup(key, thread_id);
    }

    // 2) bloom says “maybe in buffer”, so probe DPGM first
    auto v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) {
      return v;
    }
    // 3) that was a false‐positive => fall back to LIPP
    return li_.EqualityLookup(key, thread_id);
  }

  // range‐sum across both structures
  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const
  {
    return dp_.RangeQuery(lo, hi, thread_id)
         + li_.RangeQuery(lo, hi, thread_id);
  }

  // insert into DPGM + bloom + buffer, trigger flush when threshold reached
  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id)
  {
    dp_.Insert(kv, thread_id);
    bloom_.add(kv.key);

    std::lock_guard<std::mutex> lk(mu_);
    active_buf_.push_back(kv);
    if (active_buf_.size() >= flush_threshold_) {
      // swap‐out buffer, then reset DPGM+Bloom
      active_buf_.swap(flush_buf_);
      dp_.Reset();
      bloom_ = decltype(bloom_)(flush_threshold_*2, 0.01);
      cv_.notify_one();
    }
  }

  std::string name() const   { return "HybridPGMLipp"; }
  std::size_t size() const   { return dp_.size() + li_.size(); }

  bool applicable(bool unique,
                  bool rq,
                  bool ins,
                  bool mt,
                  const std::string&) const
  {
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
      // one‐shot merge
      li_.BulkMerge(batch);
      batch.clear();
    }
  }

  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                              li_;
  util::BloomFilter<KeyType>                 bloom_;

  const size_t                               flush_threshold_;
  std::vector<KeyValue<KeyType>>             active_buf_, flush_buf_;
  std::mutex                                 mu_;
  std::condition_variable                    cv_;
  bool                                       shutting_down_;
  std::thread                                flusher_;
};
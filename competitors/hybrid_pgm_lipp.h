// File: competitors/hybrid_pgm_lipp.h
#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/**
 * Hybrid: DPGM for ingestion + background flush into LIPP
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
 public:
  explicit HybridPGMLipp(const std::vector<int>& params)
    : dp_(params),
      li_(params),
      flush_threshold_(params.empty() ? 100000 : params[0]),
      shutdown_(false)
  {
    flusher_ = std::thread(&HybridPGMLipp::flushLoop, this);
  }

  virtual ~HybridPGMLipp() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      shutdown_ = true;
      cv_.notify_one();
    }
    flusher_.join();
  }

  // Build into LIPP only
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) 
  {
    uint64_t t = li_.Build(data, num_threads);
    return t;
  }

  // Lookup: try small DPGM first, then LIPP
  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const 
  {
    size_t v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) return v;
    return li_.EqualityLookup(key, thread_id);
  }

  // Range sum across both
  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const 
  {
    return dp_.RangeQuery(lo, hi, thread_id)
         + li_.RangeQuery(lo, hi, thread_id);
  }

  // Insert into DPGM + schedule buffer‐flush
  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id) 
  {
    dp_.Insert(kv, thread_id);
    {
      std::lock_guard<std::mutex> lk(mu_);
      buf_active_.push_back(kv);
      if (buf_active_.size() >= flush_threshold_) {
        buf_active_.swap(buf_flush_);
        cv_.notify_one();
        dp_.Reset();  // clear DPGM so it stays small
      }
    }
  }

  std::string name() const  { return "HybridPGMLipp"; }

  std::size_t size() const {
    // approximate total memory
    return dp_.size() + li_.size();
  }

  bool applicable(bool unique,
                  bool range_query,
                  bool insert,
                  bool multithread,
                  const std::string&) const 
  {
    return unique && !multithread;
  }

 private:
  void flushLoop() {
    std::vector<KeyValue<KeyType>> local;
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&]{ return shutdown_ || !buf_flush_.empty(); });
        if (shutdown_ && buf_flush_.empty()) break;
        buf_flush_.swap(local);
      }
      // bulk‐merge into LIPP
      li_.BulkMerge(local);
      local.clear();
    }
  }

  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                              li_;

  // Double buffer for flush batches
  std::vector<KeyValue<KeyType>> buf_active_, buf_flush_;
  size_t                         flush_threshold_;
  std::mutex                     mu_;
  std::condition_variable        cv_;
  bool                           shutdown_;
  std::thread                    flusher_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
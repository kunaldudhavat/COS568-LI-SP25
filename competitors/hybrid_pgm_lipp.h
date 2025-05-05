#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/**
 * HybridPGMLipp (Milestone 3): 
 *   • all inserts go into PGM  
 *   • once PGM’s built‐in buffer ≥ threshold, extract, sort, Reset()  
 *   • hand that batch off to a background thread that does BulkMerge into LIPP  
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

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) 
  {
    // initial bulk‐load into LIPP only
    return li_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const 
  {
    auto v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW)
      return v;
    return li_.EqualityLookup(key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const 
  {
    return dp_.RangeQuery(lo, hi, thread_id)
         + li_.RangeQuery(lo, hi, thread_id);
  }

  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id) 
  {
    dp_.Insert(kv, thread_id);

    // once PGM’s buffer is big enough, pull it out
    auto pending = dp_.ExtractBuffer();
    if (pending.size() >= flush_threshold_) {
      dp_.Reset();

      // sort ascending, so LIPP’s bulk merge is cheap
      std::sort(pending.begin(), pending.end(),
                [](auto &a, auto &b){ return a.key < b.key; });

      {
        std::lock_guard<std::mutex> lk(mu_);
        buf_flush_.insert(buf_flush_.end(),
                          std::make_move_iterator(pending.begin()),
                          std::make_move_iterator(pending.end()));
      }
      cv_.notify_one();
    }
  }

  std::string name() const  { return "HybridPGMLipp"; }
  std::size_t size() const  {
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
      // re‐sort in case multiple flushes coalesced
      std::sort(local.begin(), local.end(),
                [](auto &a, auto &b){ return a.key < b.key; });
      li_.BulkMerge(local);
      local.clear();
    }
  }

  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                              li_;

  std::vector<KeyValue<KeyType>> buf_flush_;
  size_t                         flush_threshold_;
  std::mutex                     mu_;
  std::condition_variable        cv_;
  bool                           shutdown_;
  std::thread                    flusher_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
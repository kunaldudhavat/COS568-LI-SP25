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
 * HybridPGMLipp:
 *   - Inserts go into a small DynamicPGM
 *   - Once its buffer reaches flush_threshold, swap that batch out
 *     and asynchronously merge into the LIPP bulk index
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

virtual ~HybridPGMLipp()  {
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
    // initial bulk‐load goes straight into LIPP
    return li_.Build(data, num_threads);
  }

  size_t EqualityLookup(const KeyType& key, uint32_t tid) const  {
    // try the small PGM first
    auto v = dp_.EqualityLookup(key, tid);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) return v;
    // else fall back to LIPP
    return li_.EqualityLookup(key, tid);
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t tid)  {
    dp_.Insert(kv, tid);

    std::lock_guard<std::mutex> lk(mu_);
    buf_active_.push_back(kv);
    if (buf_active_.size() >= flush_threshold_) {
      // swap into flush buffer and notify background thread
      buf_active_.swap(buf_flush_);
      cv_.notify_one();
      dp_.Reset();
    }
  }

  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi,
                      uint32_t tid) const 
  {
    return dp_.RangeQuery(lo, hi, tid) + li_.RangeQuery(lo, hi, tid);
  }

  std::string name() const  { return "HybridPGMLipp"; }
  std::size_t size() const  {
    return dp_.size() + li_.size();
  }

  bool applicable(bool unique, bool /*rq*/, bool /*ins*/,
                  bool multithread, const std::string&) const  {
    return unique && !multithread;
  }

 private:
  void flushLoop() {
    std::vector<KeyValue<KeyType>> to_flush;
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&]{ return shutdown_ || !buf_flush_.empty(); });
        if (shutdown_ && buf_flush_.empty()) break;
        buf_flush_.swap(to_flush);
      }
      // merge into the main LIPP index
      li_.BulkMerge(to_flush);
      to_flush.clear();
    }
  }

  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                             li_;

  std::vector<KeyValue<KeyType>> buf_active_, buf_flush_;
  size_t                         flush_threshold_;
  std::mutex                     mu_;
  std::condition_variable        cv_;
  bool                           shutdown_;
  std::thread                    flusher_;
};

#endif  // TLI_HYBRID_PGM_LIPP_H
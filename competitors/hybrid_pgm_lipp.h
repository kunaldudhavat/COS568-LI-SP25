#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <vector>
#include <thread>
#include <mutex>
#include <algorithm>
#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/**
 * HybridPGMLipp: async, threshold-driven flush from DPGM to LIPP.
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
 public:
  HybridPGMLipp(const std::vector<int>& params)
    : dp_(params), li_(params) {
    flush_threshold_ = params.empty() ? 100000 : params[0];
  }

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    auto t = li_.Build(data, num_threads);
    std::lock_guard<std::mutex> lock(buffer_mtx_);
    buffer_.clear();
    return t;
  }

  size_t EqualityLookup(const KeyType& key, uint32_t thread_id) const {
    auto v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) return v;
    return li_.EqualityLookup(key, thread_id);
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t thread_id) {
    {
      std::lock_guard<std::mutex> lock(buffer_mtx_);
      buffer_.push_back(kv);
      if (buffer_.size() >= flush_threshold_)
        maybe_flush_async();
    }
    dp_.Insert(kv, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi, uint32_t thread_id) const {
    auto sum = dp_.RangeQuery(lo, hi, thread_id);
    sum += li_.RangeQuery(lo, hi, thread_id);
    return sum;
  }

  std::string name() const { return "HybridPGMLipp"; }
  std::size_t size() const { return dp_.size() + li_.size(); }

 private:
  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                             li_;
  mutable std::mutex                       buffer_mtx_;
  std::vector<KeyValue<KeyType>>           buffer_;
  size_t                                   flush_threshold_;

  void flush() {
    std::vector<KeyValue<KeyType>> to_flush;
    {
      std::lock_guard<std::mutex> lock(buffer_mtx_);
      to_flush.swap(buffer_);
    }
    std::sort(to_flush.begin(), to_flush.end(),
              [](auto &a, auto &b){ return a.key < b.key; });
    li_.BulkMerge(to_flush);
  }

  void maybe_flush_async() {
    std::thread([this]{ this->flush(); }).detach();
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H
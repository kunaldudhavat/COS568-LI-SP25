#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <vector>
#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

// A simple hybrid index: DPGM for inserts, LIPP for lookups, with naive flush
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
public:
  explicit HybridPGMLipp(const std::vector<int>& params)
    : dp_(params), li_(params)
  {
    flush_threshold_ = params.empty() ? 100000 : params[0];
  }

  virtual ~HybridPGMLipp() { }

  // Bulk-load all initial data into LIPP
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads)
  {
    uint64_t t = li_.Build(data, num_threads);
    buffer_.clear();
    return t;
  }

  // First check DPGM, then fall back to LIPP
  size_t EqualityLookup(const KeyType& key, uint32_t thread_id) const
  {
    size_t v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) return v;
    return li_.EqualityLookup(key, thread_id);
  }

  // Insert into DPGM buffer and model; flush when threshold reached
  void Insert(const KeyValue<KeyType>& kv, uint32_t thread_id)
  {
    buffer_.push_back(kv);
    dp_.Insert(kv, thread_id);
    if (buffer_.size() >= flush_threshold_) flush();
  }

  // RangeQuery across both structures
  uint64_t RangeQuery(const KeyType& lower,
                      const KeyType& upper,
                      uint32_t thread_id) const
  {
    uint64_t res = dp_.RangeQuery(lower, upper, thread_id);
    res += li_.RangeQuery(lower, upper, thread_id);
    return res;
  }

  std::string name() const
  {
    return "HybridPGMLipp";
  }

  // Memory footprint = sum of both indexes
  std::size_t size() const
  {
    return dp_.size() + li_.size();
  }

private:
  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                              li_;
  std::vector<KeyValue<KeyType>>             buffer_;
  size_t                                     flush_threshold_;

  // Naive flush: migrate all buffered keys to LIPP, then clear DPGM
  void flush() {
    for (const auto &kv : buffer_) {
      li_.Insert(kv, /*thread_id=*/0);
    }
    buffer_.clear();
    dp_ = DynamicPGM<KeyType, SearchClass, pgm_error>(std::vector<int>{});
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H
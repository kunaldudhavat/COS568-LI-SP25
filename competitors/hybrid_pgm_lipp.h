// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <vector>
#include "../util.h"
#include "../utils/bloom_filter.hpp"  // your BloomFilter
#include "base.h"
#include "dynamic_pgm_index.h"         // DynamicPGM
#include "lipp.h"                      // LIPP wrapper

/**
 * HybridPGMLipp<…> :
 *  - INSERTs go into a tiny DynamicPGM + a BloomFilter + a buffer
 *  - once buffer ≥ flush_threshold, synchronously BulkMerge(buffer) into LIPP
 *  - LOOKUP: if bloom_.contains(key) then try dp_; otherwise skip dp_ and go straight to LIPP
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
public:
  explicit HybridPGMLipp(const std::vector<int>& params)
    : dp_(params),
      li_(params),
      flush_threshold_( params.empty() ? 2000000 : params[0] ),
      bloom_(flush_threshold_, /*fp_rate=*/0.01)
  {}

  // bulk‐load initial dataset into LIPP
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) 
  {
    return li_.Build(data, num_threads);
  }

  // try the tiny DPGM only if bloom says “maybe”—else go direct to LIPP
  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const 
  {
    if (bloom_.contains(key)) {
      auto v = dp_.EqualityLookup(key, thread_id);
      if (v != util::NOT_FOUND && v != util::OVERFLOW)
        return v;
    }
    return li_.EqualityLookup(key, thread_id);
  }

  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const 
  {
    // rare for mixed workloads, but include both sides
    return dp_.RangeQuery(lo, hi, thread_id)
         + li_.RangeQuery(lo, hi, thread_id);
  }

  // insert into DPGM + bloom + buffer; flush synchronously when threshold hit
  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id) 
  {
    dp_.Insert(kv, thread_id);
    bloom_.add(kv.key);
    buffer_.push_back(kv);

    if (buffer_.size() >= flush_threshold_) {
      // take the batch, reset DPGM & bloom, merge into LIPP
      std::vector<KeyValue<KeyType>> batch = std::move(buffer_);
      buffer_.clear();
      dp_.Reset();
      bloom_.clear();
      li_.BulkMerge(batch);
    }
  }

  std::string name() const  { return "HybridPGMLipp"; }
  std::size_t size() const  { return dp_.size() + li_.size(); }

  bool applicable(bool unique,
                  bool rq,
                  bool ins,
                  bool mt,
                  const std::string&) const 
  {
    // only for unique, single-threaded mixed workloads
    return unique && !mt;
  }

private:
  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                               li_;
  util::BloomFilter<KeyType>                  bloom_;
  const size_t                                flush_threshold_;
  std::vector<KeyValue<KeyType>>              buffer_;
};
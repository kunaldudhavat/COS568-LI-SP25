// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <algorithm>

#include "../util.h"
#include "../utils/bloom_filter.hpp"   // your BloomFilter
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/**
 * HybridPGMLipp with per-segment BloomFilters:
 *
 *  - dp_: tiny DynamicPGM + buffer
 *  - segments_: immutable LIPP segments
 *  - blooms_: one BloomFilter per segment
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
public:
  explicit HybridPGMLipp(const std::vector<int>& params)
    : dp_(params),
      flush_threshold_( params.empty() ? 200000 : params[0] )
  {}

  ~HybridPGMLipp() { /* no threads to join here */ }

  // 1) Bulk‐load initial data into base segment + its bloom
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) 
  {
    std::lock_guard<std::mutex> g(mu_);
    segments_.clear();
    blooms_.clear();

    // base LIPP
    auto base = std::make_unique<Lipp<KeyType>>(std::vector<int>{});
    base->Build(data, num_threads);

    // base bloom
    util::BloomFilter<KeyType> bf(data.size(), 0.01);
    for (auto &kv : data) bf.add(kv.key);

    segments_.push_back(std::move(base));
    blooms_.push_back(std::move(bf));

    return 0; // time reported inside Lipp::Build
  }

  // 2) Lookup: DPGM → each segment via bloom → LIPP
  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const 
  {
    auto v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW)
      return v;

    std::lock_guard<std::mutex> g(mu_);
    for (size_t i = 0; i < segments_.size(); i++) {
      if (!blooms_[i].contains(key)) continue;
      auto r = segments_[i]->EqualityLookup(key, thread_id);
      if (r != util::NOT_FOUND) return r;
    }
    return util::NOT_FOUND;
  }

  // 3) Range queries still scan all segments
  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const 
  {
    uint64_t sum = dp_.RangeQuery(lo, hi, thread_id);
    std::lock_guard<std::mutex> g(mu_);
    for (auto const &seg : segments_)
      sum += seg->RangeQuery(lo, hi, thread_id);
    return sum;
  }

  // 4) Insert into DPGM + buffer + current bloom; flush→new segment+bloom
  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id) 
  {
    dp_.Insert(kv, thread_id);

    std::unique_lock<std::mutex> lk(mu_);
    buffer_.push_back(kv);
    // also add to the *latest* bloom (the one backing dp_ contents)
    // we treat dp_ itself as segment[-1] for bloom purposes
    if (buffer_.size() < flush_threshold_) return;

    // steal batch
    auto batch = std::move(buffer_);
    buffer_.clear();
    lk.unlock();

    // background build a new segment + bloom
    std::thread([this, batch = std::move(batch)]() mutable {
      // 1) build new LIPP segment
      auto seg = std::make_unique<Lipp<KeyType>>(std::vector<int>{});
      seg->BulkMerge(batch);

      // 2) build its bloom
      util::BloomFilter<KeyType> bf(batch.size(), 0.01);
      for (auto &kv : batch) bf.add(kv.key);

      // 3) publish under lock
      std::lock_guard<std::mutex> g(mu_);
      segments_.push_back(std::move(seg));
      blooms_.push_back(std::move(bf));
    }).detach();
  }

  std::string name() const  { return "HybridPGMLipp"; }

  std::size_t size() const  {
    std::size_t s = dp_.size();
    std::lock_guard<std::mutex> g(mu_);
    for (auto const &seg : segments_) s += seg->size();
    return s;
  }

  bool applicable(bool unique, bool rq, bool ins, bool mt,
                  const std::string&) const 
  {
    return unique && !mt;
  }

private:
  const size_t flush_threshold_;
  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;

  mutable std::mutex
                          mu_;
  std::vector<std::unique_ptr<Lipp<KeyType>>> segments_;
  std::vector<util::BloomFilter<KeyType>>     blooms_;
  std::vector<KeyValue<KeyType>>             buffer_;
};
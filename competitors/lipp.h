#pragma once

#include "./lipp/src/core/lipp.h"
#include "base.h"

#include <algorithm>
#include <vector>

/**
 * LIPP wrapper: add true bulk‐merge support for batches of keys.
 */
template<class KeyType>
class Lipp : public Base<KeyType> {
 public:
  Lipp(const std::vector<int>& params) {}

  // Build from scratch
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    std::vector<std::pair<KeyType, uint64_t>> loading;
    loading.reserve(data.size());
    for (auto &kv : data) loading.emplace_back(kv.key, kv.value);
    return util::timing([&] {
      lipp_.bulk_load(loading.data(), loading.size());
    });
  }

  // Point‐lookup
  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    uint64_t value;
    if (!lipp_.find(lookup_key, value)) return util::NOT_FOUND;
    return value;
  }

  // Range query
  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi, uint32_t thread_id) const {
    auto it = lipp_.lower_bound(lo);
    uint64_t sum = 0;
    while (it != lipp_.end() && it->comp.data.key <= hi) {
      sum += it->comp.data.value;
      ++it;
    }
    return sum;
  }

  // Insert single key
  void Insert(const KeyValue<KeyType>& kv, uint32_t thread_id) {
    lipp_.insert(kv.key, kv.value);
  }

  std::string name() const { return "LIPP"; }
  std::size_t size() const  { return lipp_.index_size(); }

  bool applicable(bool unique,
                  bool range_query,
                  bool insert,
                  bool multithread,
                  const std::string&) const {
    return unique && !multithread;
  }

  /**
   * True bulk‐merge:
   *  1) pull out all existing (key,value) pairs,
   *  2) append the new sorted batch,
   *  3) sort the combined list,
   *  4) rebuild via a single bulk_load().
   */
  void BulkMerge(const std::vector<KeyValue<KeyType>>& batch) {
    // 1) extract all existing (key,value) pairs
    auto all = lipp_.extract_all();  // std::vector<std::pair<KeyType,uint64_t>>
    // 2) append the new batch, converting each packed KeyValue to an unpacked pair
    all.reserve(all.size() + batch.size());
    for (auto &kv : batch) {
      all.emplace_back( static_cast<KeyType>(kv.key),
                        static_cast<uint64_t>(kv.value) );
    }
    // 3) sort by key
    std::sort(all.begin(), all.end(),
              [](auto &a, auto &b){ return a.first < b.first; });
    // 4) rebuild in one shot
    util::timing([&]{
      lipp_.bulk_load(all.data(), all.size());
    });
  }

 private:
  LIPP<KeyType, uint64_t> lipp_;
};
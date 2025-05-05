#pragma once

#include "./lipp/src/core/lipp.h"
#include "base.h"

/**
 * LIPP wrapper: bulk‐load + bulk‐merge support.
 */
template<class KeyType>
class Lipp : public Base<KeyType> {
 public:
  explicit Lipp(const std::vector<int>& params) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t num_threads) 
  {
    std::vector<std::pair<KeyType, uint64_t>> loading;
    loading.reserve(data.size());
    for (auto &kv : data) loading.emplace_back(kv.key, kv.value);
    return util::timing([&] {
      lipp_.bulk_load(loading.data(), loading.size());
    });
  }

  size_t EqualityLookup(const KeyType& key,
                        uint32_t thread_id) const 
  {
    uint64_t v;
    return lipp_.find(key, v) ? v : util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t thread_id) const 
  {
    auto it = lipp_.lower_bound(lo);
    uint64_t sum = 0;
    while (it != lipp_.end() && it->comp.data.key <= hi) {
      sum += it->comp.data.value;
      ++it;
    }
    return sum;
  }

  void Insert(const KeyValue<KeyType>& kv,
              uint32_t thread_id) 
  {
    lipp_.insert(kv.key, kv.value);
  }

  std::string name() const  { return "LIPP"; }
  std::size_t size() const  { return lipp_.index_size(); }

  bool applicable(bool unique,
                  bool range_query,
                  bool insert,
                  bool multithread,
                  const std::string&) const 
  {
    return unique && !multithread;
  }

  /// Naïve append of a sorted batch into an existing index.
  void BulkMerge(const std::vector<KeyValue<KeyType>>& batch) {
    std::vector<std::pair<KeyType, uint64_t>> raw;
    raw.reserve(batch.size());
    for (auto &kv : batch) raw.emplace_back(kv.key, kv.value);
    lipp_.bulk_load_append(raw.data(), raw.size());
  }

 private:
  LIPP<KeyType, uint64_t> lipp_;
};
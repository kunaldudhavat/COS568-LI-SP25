#pragma once

#include "./lipp/src/core/lipp.h"
#include "base.h"

/**
 * LIPP wrapper: bulk‐load + point/range queries + naïve batch‐merge
 */
template<class KeyType>
class Lipp : public Base<KeyType> {
 public:
  explicit Lipp(const std::vector<int>& /*params*/) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t /*num_threads*/) 
  {
    std::vector<std::pair<KeyType,uint64_t>> loading;
    loading.reserve(data.size());
    for (auto &kv : data) loading.emplace_back(kv.key, kv.value);

    return util::timing([&] {
      lipp_.bulk_load(loading.data(), loading.size());
    });
  }

  size_t EqualityLookup(const KeyType& k, uint32_t) const  {
    uint64_t v;
    if (!lipp_.find(k, v)) return util::NOT_FOUND;
    return v;
  }

  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi,
                      uint32_t) const 
  {
    auto it = lipp_.lower_bound(lo);
    uint64_t sum = 0;
    while (it != lipp_.end() && it->comp.data.key <= hi) {
      sum += it->comp.data.value;
      ++it;
    }
    return sum;
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t)  {
    lipp_.insert(kv.key, kv.value);
  }

  std::string name() const  { return "LIPP"; }
  std::size_t size() const  { return lipp_.index_size(); }

  bool applicable(bool unique, bool /*range_query*/,
                  bool /*insert*/, bool multithread,
                  const std::string&) const  {
    return unique && !multithread;
  }

  /// Naïvely merge a batch of sorted kv‐pairs into the existing LIPP
  void BulkMerge(const std::vector<KeyValue<KeyType>>& batch) {
    for (auto &kv : batch) {
      lipp_.insert(kv.key, kv.value);
    }
  }

 private:
  LIPP<KeyType,uint64_t> lipp_;
};
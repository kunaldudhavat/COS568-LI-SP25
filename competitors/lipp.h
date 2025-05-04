#pragma once

#include "./lipp/src/core/lipp.h"
#include "base.h"

/**
 * LIPP wrapper: add bulk-merge support for batches of keys.
 */
template<class KeyType>
class Lipp : public Base<KeyType> {
 public:
  Lipp(const std::vector<int>& params) {}

  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) override {
    std::vector<std::pair<KeyType, uint64_t>> loading_data;
    loading_data.reserve(data.size());
    for (auto &itm : data)
      loading_data.emplace_back(itm.key, itm.value);
    return util::timing([&] {
      lipp_.bulk_load(loading_data.data(), loading_data.size());
    });
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const override {
    uint64_t value;
    if (!lipp_.find(lookup_key, value)) return util::NOT_FOUND;
    return value;
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const override {
    auto it = lipp_.lower_bound(lower_key);
    uint64_t result = 0;
    while (it != lipp_.end() && it->comp.data.key <= upper_key) {
      result += it->comp.data.value;
      ++it;
    }
    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) override {
    lipp_.insert(data.key, data.value);
  }

  std::string name() const override { return "LIPP"; }
  std::size_t size() const override { return lipp_.index_size(); }
  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string&) const override {
    return unique && !multithread;
  }

  /**
   * Bulk merge a sorted batch into the existing index.
   * Requires bulk_load_append() in core LIPP.
   */
  void BulkMerge(const std::vector<KeyValue<KeyType>>& batch) {
    std::vector<std::pair<KeyType, uint64_t>> raw;
    raw.reserve(batch.size());
    for (auto &kv : batch) raw.emplace_back(kv.key, kv.value);
    lipp_.bulk_load_append(raw.data(), raw.size());  // add this API
  }

 private:
  LIPP<KeyType, uint64_t> lipp_;
};
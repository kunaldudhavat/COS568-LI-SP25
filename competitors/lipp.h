#pragma once

#include "./lipp/src/core/lipp.h"
#include "base.h"

template<class KeyType>
class Lipp : public Base<KeyType> {
public:
  Lipp(const std::vector<int>& params) {}

  // build from scratch
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t /*num_threads*/) {
    std::vector<std::pair<KeyType,uint64_t>> raw;
    raw.reserve(data.size());
    for (auto &kv : data) raw.emplace_back(kv.key, kv.value);
    return util::timing([&] {
      core_.bulk_load(raw.data(), raw.size());
    });
  }

  size_t EqualityLookup(const KeyType& key, uint32_t /*tid*/) const {
    uint64_t v;
    return core_.find(key, v) ? v : util::NOT_FOUND;
  }

  uint64_t RangeQuery(const KeyType& lo,
                      const KeyType& hi,
                      uint32_t /*tid*/) const {
    auto it = core_.lower_bound(lo);
    uint64_t sum = 0;
    while (it != core_.end() && it->comp.data.key <= hi) {
      sum += it->comp.data.value;
      ++it;
    }
    return sum;
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t /*tid*/) {
    core_.insert(kv.key, kv.value);
  }

  std::string name() const    { return "LIPP"; }
  std::size_t size() const    { return core_.index_size(); }
  bool applicable(bool unique, bool range, bool ins, bool mt, const std::string&)
    const { return unique && !mt; }

  /**
   * Bulk‐merge [batch] into the existing index in O(N + M) instead of O(N·M).
   */
  void BulkMerge(const std::vector<KeyValue<KeyType>>& batch) {
    // 1) extract current content
    std::vector<KeyValue<KeyType>> all;
    all.reserve(core_.index_size());  // ≈ number of entries
    extract_all(core_.root, all);

    // 2) append new batch
    all.insert(all.end(), batch.begin(), batch.end());

    // 3) sort
    std::sort(all.begin(), all.end(),
              [](auto &a, auto &b){ return a.key < b.key; });

    // 4) rebuild in one shot
    std::vector<std::pair<KeyType,uint64_t>> raw;
    raw.reserve(all.size());
    for (auto &kv : all) raw.emplace_back(kv.key, kv.value);

    util::timing([&] {
      core_.bulk_load(raw.data(), raw.size());
    });
  }

private:
  // recursive in‐order traversal
  void extract_all(typename LIPP<KeyType,uint64_t>::Node* node,
                   std::vector<KeyValue<KeyType>>& out) {
    if (!node) return;
    for (int i = 0; i < node->num_items; ++i) {
      if (BITMAP_GET(node->child_bitmap, i)) {
        extract_all(node->items[i].comp.child, out);
      } else if (!BITMAP_GET(node->none_bitmap, i)) {
        KeyValue<KeyType> kv{ node->items[i].comp.data.key,
                              node->items[i].comp.data.value };
        out.push_back(kv);
      }
    }
  }

  // the real LIPP
  LIPP<KeyType, uint64_t> core_;
};
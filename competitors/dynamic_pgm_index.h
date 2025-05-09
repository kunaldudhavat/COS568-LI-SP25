#ifndef TLI_DYNAMIC_PGM_H
#define TLI_DYNAMIC_PGM_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"  // your PGM‐dynamic library

/**
 * DynamicPGM: approximate index + dynamic updates.
 * We only add Reset() to clear it completely.
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class DynamicPGM : public Competitor<KeyType, SearchClass> {
 public:
  explicit DynamicPGM(const std::vector<int>& /*params*/) {}

  // Build from scratch
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data,
                 size_t /*num_threads*/) {
    std::vector<std::pair<KeyType,uint64_t>> loading;
    loading.reserve(data.size());
    for (auto &kv : data) loading.emplace_back(kv.key, kv.value);

    uint64_t t = util::timing([&] {
      pgm_ = decltype(pgm_)(loading.begin(), loading.end());
    });
    return t;
  }

  size_t EqualityLookup(const KeyType& key, uint32_t /*thread_id*/) const  {
    auto it = pgm_.find(key);
    if (it == pgm_.end()) return util::NOT_FOUND;
    return it->value();
  }

  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi,
                      uint32_t /*thread_id*/) const  {
    auto it = pgm_.lower_bound(lo);
    uint64_t sum = 0;
    while (it != pgm_.end() && it->key() <= hi) {
      sum += it->value();
      ++it;
    }
    return sum;
  }

  void Insert(const KeyValue<KeyType>& kv, uint32_t /*thread_id*/)  {
    pgm_.insert(kv.key, kv.value);
  }

  std::string name() const  { return "DynamicPGM"; }
  std::size_t size() const  { return pgm_.size_in_bytes(); }

  bool applicable(bool /*unique*/,
                  bool /*range_query*/,
                  bool /*insert*/,
                  bool /*multithread*/,
                  const std::string&) const  {
    return true;  // single‐thread only
  }

  std::vector<std::string> variants() const  {
    return { SearchClass::name(), std::to_string(pgm_error) };
  }

  /// Completely clear out the PGM model
  void Reset() {
    pgm_ = decltype(pgm_)();
  }

 private:
  DynamicPGMIndex<
    KeyType, uint64_t,
    SearchClass,
    PGMIndex<KeyType,SearchClass,pgm_error,16>
  > pgm_;
};

#endif  // TLI_DYNAMIC_PGM_H
// File: competitors/dynamic_pgm_index.h
#ifndef TLI_DYNAMIC_PGM_H
#define TLI_DYNAMIC_PGM_H

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"  // from PGM dynamic library

/**
 * DynamicPGM: supports approximate indexing + dynamic updates.
 * Added ExtractBuffer() to pull out recently inserted keys in one batch.
 */
template <class KeyType, class SearchClass, size_t pgm_error>
class DynamicPGM : public Competitor<KeyType, SearchClass> {
 public:
  DynamicPGM(const std::vector<int>& params) {}

  // Removed 'override' here
  uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
    std::vector<std::pair<KeyType, uint64_t>> loading_data;
    loading_data.reserve(data.size());
    for (const auto& itm : data) {
      loading_data.emplace_back(itm.key, itm.value);
    }

    uint64_t build_time = util::timing([&] {
      pgm_ = decltype(pgm_)(loading_data.begin(), loading_data.end());
    });
    return build_time;
  }

  size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
    auto it = pgm_.find(lookup_key);
    if (it == pgm_.end()) return util::NOT_FOUND;
    return it->value();
  }

  uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
    auto it = pgm_.lower_bound(lower_key);
    uint64_t result = 0;
    while (it != pgm_.end() && it->key() <= upper_key) {
      result += it->value();
      ++it;
    }
    return result;
  }

  void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
    pgm_.insert(data.key, data.value);
  }

  std::string name() const { return "DynamicPGM"; }
  std::size_t size() const { return pgm_.size_in_bytes(); }

  bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
    return !multithread;
  }

  std::vector<std::string> variants() const {
    return { SearchClass::name(), std::to_string(pgm_error) };
  }

  /**
   * Extract and clear the library's internal insertion buffer.
   * Requires you to add extract_buffer() to pgm_index_dynamic.hpp.
   */
  std::vector<KeyValue<KeyType>> ExtractBuffer() {
    auto raw = pgm_.extract_buffer();  // add this API
    std::vector<KeyValue<KeyType>> out;
    out.reserve(raw.size());
    for (auto &p : raw) out.push_back({p.first, p.second});
    return out;
  }

 private:
  DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_;
};

#endif  // TLI_DYNAMIC_PGM_H
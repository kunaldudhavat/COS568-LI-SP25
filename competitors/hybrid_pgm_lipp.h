// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <algorithm>
#include "dynamic_pgm_index.h"   // your DPGM impl
#include "lipp.h"                // your LIPP impl

template <
    typename Key,
    typename Value,
    typename Searcher,
    size_t PGMError = 16
>
class HybridPGMLipp {
public:
  // params[0] = flush threshold; default to 2 000 000 when nothing specified
  explicit HybridPGMLipp(const std::vector<int>& params)
    : _flush_threshold(
        params.empty() 
          ? 2'000'000u 
          : static_cast<size_t>(params[0])
      ),
      _dpgm(params),
      _buffer()
  {
    // start with an empty LIPP
    _lipp_ptr.store(new LIPP<Key,Value,true>(), std::memory_order_release);
  }

  ~HybridPGMLipp() {
    delete _lipp_ptr.load(std::memory_order_acquire);
  }

  // Bulk‐load at startup
  uint64_t Build(const std::vector<KeyValue<Key>>& data,
                 size_t num_threads)
  {
    return _lipp_ptr.load(std::memory_order_acquire)
            ->Build(data, num_threads);
  }

  // POINT‐LOOKUP: **first** try big LIPP, **then** small DPGM
  size_t EqualityLookup(const Key& key, uint32_t thread_id) const {
    Value v;
    auto* big = _lipp_ptr.load(std::memory_order_acquire);
    if (big->EqualityLookup(key, thread_id) != util::NOT_FOUND)
      return big->EqualityLookup(key, thread_id);

    // fallback to the recent‐inserts buffer
    return _dpgm.EqualityLookup(key, thread_id);
  }

  // RANGE‐QUERY across both
  uint64_t RangeQuery(const Key& lo, const Key& hi,
                      uint32_t thread_id) const
  {
    auto* big = _lipp_ptr.load(std::memory_order_acquire);
    return big->RangeQuery(lo, hi, thread_id)
         + _dpgm.RangeQuery(lo, hi, thread_id);
  }

  // INSERT into DPGM + buffer, flush when threshold reached
  void Insert(const KeyValue<Key>& kv, uint32_t thread_id) {
    _dpgm.Insert(kv, thread_id);

    {
      std::lock_guard<std::mutex> g(_mu);
      _buffer.push_back(kv);
    }
    if (_buffer.size() >= _flush_threshold) {
      flush_now();
    }
  }

  std::string name() const { return "HybridPGMLipp"; }
  std::size_t size() const {
    auto* big = _lipp_ptr.load(std::memory_order_acquire);
    return big->size() + _dpgm.size();
  }

private:
  void flush_now() {
    // grab and clear the buffer
    std::vector<KeyValue<Key>> batch;
    {
      std::lock_guard<std::mutex> g(_mu);
      batch.swap(_buffer);
    }
    // reset the DPGM entirely
    _dpgm = DynamicPGM<Key,Searcher,PGMError>(std::vector<int>());

    // merge into a fresh LIPP
    auto* old = _lipp_ptr.load(std::memory_order_acquire);
    // extract all old entries
    std::vector<std::pair<Key,Value>> all;
    old->extract_all(all);
    // append and sort the new batch
    for (auto &kv : batch)
      all.emplace_back(kv.key, kv.value);
    std::sort(all.begin(), all.end(),
              [](auto &a, auto &b){ return a.first < b.first; });
    // build a fresh LIPP
    auto* fresh = new LIPP<Key,Value,true>();
    fresh->bulk_load(all.data(), int(all.size()));
    // swap
    _lipp_ptr.store(fresh, std::memory_order_release);
    delete old;
  }

  const size_t _flush_threshold;
  DynamicPGM<Key,Searcher,PGMError>     _dpgm;
  std::atomic<LIPP<Key,Value,true>*>    _lipp_ptr;
  std::mutex                            _mu;
  std::vector<KeyValue<Key>>            _buffer;
};
// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <atomic>
#include <mutex>
#include <thread>
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
  // 1) Primary constructor: threshold in keys before background‐flush
  explicit HybridPGMLipp(size_t flush_threshold = 100000)
    : _flush_threshold(flush_threshold),
      _dpgm(),
      _lipp_ptr(nullptr),
      _shutdown(false)
  {
    // bootstrap an empty LIPP
    _lipp_ptr.store(new LIPP<Key,Value,true>(), std::memory_order_release);
  }

  // 2) Overload for the benchmark harness (vector<int> params)
  //    it just picks params[0] as your threshold.
  explicit HybridPGMLipp(const std::vector<int>& params)
    : HybridPGMLipp( params.empty() ? 100000u
                                    : static_cast<size_t>(params[0]) )
  {}

  ~HybridPGMLipp() {
    _shutdown.store(true, std::memory_order_relaxed);
    // no thread to join here: flush happens inline on swap
    delete _lipp_ptr.load(std::memory_order_acquire);
  }

  // INSERT: into tiny in‐memory DPGM + buffer
  void Insert(const Key& k, const Value& v) {
    _dpgm.insert({k, v});
    {
      std::lock_guard<std::mutex> g(_buffer_mu);
      _buffer.emplace_back(k,v);
    }
    if (_buffer.size() >= _flush_threshold) {
      flush_now();
    }
  }

  // LOOKUP: try small DPGM first, then big LIPP
  bool find(const Key& k, Value& out) const {
    if (_dpgm.find(k, out)) return true;
    auto ptr = _lipp_ptr.load(std::memory_order_acquire);
    out = ptr->at(k);
    return true;
  }

private:
  // immediately flush on calling thread
  void flush_now() {
    // steal buffer
    std::vector<std::pair<Key,Value>> batch;
    {
      std::lock_guard<std::mutex> g(_buffer_mu);
      batch.swap(_buffer);
    }
    _dpgm = {};                // reset tiny DPGM
    // merge + rebuild
    background_flush(std::move(batch));
  }

  // do the heavy merge+bulk_load
  void background_flush(std::vector<std::pair<Key,Value>> batch) {
    // 1) grab old LIPP snapshot
    auto old = _lipp_ptr.load(std::memory_order_acquire);

    // 2) extract sorted old contents
    std::vector<std::pair<Key,Value>> all;
    all.reserve(old->index_size());
    old->extract_all_rec(old->get_root(), all);

    // 3) sort batch
    std::sort(batch.begin(), batch.end(),
              [](auto &a, auto &b){ return a.first < b.first; });

    // 4) merge two sorted arrays
    std::vector<Key>   keys;
    std::vector<Value> vals;
    keys.reserve(all.size() + batch.size());
    vals.reserve(all.size() + batch.size());
    size_t i=0, j=0;
    while(i<all.size() && j<batch.size()) {
      if (all[i].first < batch[j].first) {
        keys.push_back(all[i].first);
        vals.push_back(all[i].second);
        ++i;
      } else {
        keys.push_back(batch[j].first);
        vals.push_back(batch[j].second);
        ++j;
      }
    }
    while(i<all.size()) { keys.push_back(all[i].first);  vals.push_back(all[i].second);  ++i; }
    while(j<batch.size()){ keys.push_back(batch[j].first); vals.push_back(batch[j].second); ++j; }

    // 5) build a brand‐new LIPP
    auto fresh = new LIPP<Key,Value,true>();
    fresh->bulk_load(
      reinterpret_cast<std::pair<Key,Value>*>(keys.data()),
      int(keys.size())
    );

    // 6) atomically swap in
    _lipp_ptr.store(fresh, std::memory_order_release);

    // 7) delete old
    delete old;
  }

private:
  const size_t _flush_threshold;

  // tiny, write‐buffer model
  DynamicPGM<Key,Value,Searcher,PGMError> _dpgm;

  // big, read‐optimized LIPP pointer
  std::atomic<LIPP<Key,Value,true>*> _lipp_ptr;

  // insertion buffer
  mutable std::mutex _buffer_mu;
  std::vector<std::pair<Key,Value>> _buffer;

  // shutdown flag (not strictly needed here)
  std::atomic<bool> _shutdown;
};
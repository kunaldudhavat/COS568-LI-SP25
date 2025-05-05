// File: competitors/hybrid_pgm_lipp.h
#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>
#include <algorithm>

#include "dynamic_pgm_index.h"   // your DPGM impl
#include "lipp.h"                // the core LIPP<T,P,USE_FMCD> class

template <
    typename Key,
    typename Value,
    typename Searcher,
    size_t PGMError = 16
>
class HybridPGMLipp {
public:
  // 1) Main constructor: take a vector<int> of params[0]=flush_threshold
  explicit HybridPGMLipp(const std::vector<int>& params)
    : _flush_threshold(
        params.empty()
          ? 100000u
          : static_cast<size_t>(params[0])
      ),
      // ** pass the params vector into DynamicPGM **
      _dpgm(params),
      _lipp_ptr(nullptr),
      _shutdown(false)
  {
    // bootstrap an empty core‐LIPP
    _lipp_ptr.store(new LIPP<Key,Value,true>(),
                    std::memory_order_release);
  }

  // 2) Fallback if benchmark harness calls size_t‐only ctor
  explicit HybridPGMLipp(size_t flush_threshold)
    : _flush_threshold(flush_threshold),
      // give DynamicPGM an empty param list
      _dpgm(std::vector<int>()),
      _lipp_ptr(nullptr),
      _shutdown(false)
  {
    _lipp_ptr.store(new LIPP<Key,Value,true>(),
                    std::memory_order_release);
  }

  ~HybridPGMLipp() {
    _shutdown.store(true, std::memory_order_relaxed);
    delete _lipp_ptr.load(std::memory_order_acquire);
  }

  // INSERT: into tiny in‐memory DPGM + buffer
  void Insert(const Key& k, const Value& v) {
    _dpgm.insert({k, v});
    {
      std::lock_guard<std::mutex> g(_buffer_mu);
      _buffer.emplace_back(k, v);
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
    std::vector<std::pair<Key,Value>> batch;
    {
      std::lock_guard<std::mutex> g(_buffer_mu);
      batch.swap(_buffer);
    }
    _dpgm = DynamicPGM<Key,Searcher,PGMError>(std::vector<int>()); // reset DPGM
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

  // ** fixed: only 3 template args here **
  DynamicPGM<Key,Searcher,PGMError>     _dpgm;

  // big, read‐optimized LIPP pointer
  std::atomic<LIPP<Key,Value,true>*>    _lipp_ptr;

  // insertion buffer
  mutable std::mutex                    _buffer_mu;
  std::vector<std::pair<Key,Value>>     _buffer;

  // shutdown flag
  std::atomic<bool>                     _shutdown;
};
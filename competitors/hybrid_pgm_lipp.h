#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>
#include <algorithm>
#include <string>

#include "dynamic_pgm_index.h"  // your DPGM implementation
#include "lipp.h"               // your LIPP implementation

// HybridPGMLipp:
//  - collects inserts into a small in-memory DPGM;
//  - once the buffer hits a threshold, it flushes that batch into a read-optimized LIPP via bulk-merge.

template <
    typename Key,
    typename Value,
    typename Searcher,
    size_t PGMError = 16
>
class HybridPGMLipp {
public:
  // Construct from benchmark params[0] = flush threshold
  explicit HybridPGMLipp(const std::vector<int>& params)
    : _flush_threshold(params.empty() ? 100000u : static_cast<size_t>(params[0])),
      _dpgm(params),
      _lipp_ptr(nullptr),
      _shutdown(false)
  {
    // bootstrap an empty LIPP
    _lipp_ptr.store(new LIPP<Key,Value,true>(), std::memory_order_release);
  }

  // Fallback constructor: supply flush threshold directly
  explicit HybridPGMLipp(size_t flush_threshold)
    : _flush_threshold(flush_threshold),
      _dpgm(std::vector<int>()),
      _lipp_ptr(nullptr),
      _shutdown(false)
  {
    _lipp_ptr.store(new LIPP<Key,Value,true>(), std::memory_order_release);
  }

  ~HybridPGMLipp() {
    _shutdown.store(true, std::memory_order_relaxed);
    delete _lipp_ptr.load(std::memory_order_acquire);
  }

  // Insert into DPGM and buffer; flush when threshold reached
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

  // Lookup: try DPGM first, then LIPP
  bool find(const Key& k, Value& out) const {
    if (_dpgm.find(k, out)) return true;
    auto ptr = _lipp_ptr.load(std::memory_order_acquire);
    out = ptr->at(k);
    return true;
  }

  // ────────── Benchmark harness hooks ────────────────────────────────────

  std::string name() const {
    return "HybridPGMLipp";
  }

  std::size_t size() const {
    auto ptr = _lipp_ptr.load(std::memory_order_acquire);
    return _dpgm.size_in_bytes() + ptr->index_size();
  }

  bool applicable(bool unique,
                  bool range_query,
                  bool insert,
                  bool multithread,
                  const std::string&) const {
    // only use in single-threaded, unique-key workloads
    return unique && !multithread;
  }

  std::vector<std::string> variants() const {
    return {
      "threshold=" + std::to_string(_flush_threshold),
      "error="     + std::to_string(PGMError)
    };
  }

private:
  // Immediate flush on caller thread
  void flush_now() {
    std::vector<std::pair<Key,Value>> batch;
    {
      std::lock_guard<std::mutex> g(_buffer_mu);
      batch.swap(_buffer);
    }
    // reset the DPGM
    _dpgm = DynamicPGM<Key,Searcher,PGMError>(std::vector<int>());
    background_flush(std::move(batch));
  }

  // Merge buffer into LIPP via bulk-load
  void background_flush(std::vector<std::pair<Key,Value>> batch) {
    // take snapshot of old LIPP
    auto old = _lipp_ptr.load(std::memory_order_acquire);

    // extract and sort existing data
    std::vector<std::pair<Key,Value>> all;
    all.reserve(old->index_size());
    old->extract_all_rec(old->get_root(), all);

    std::sort(batch.begin(), batch.end(),
              [](auto &a, auto &b){ return a.first < b.first; });

    // merge two sorted arrays of (key,value)
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

    // build a fresh LIPP
    auto fresh = new LIPP<Key,Value,true>();
    fresh->bulk_load(
      reinterpret_cast<std::pair<Key,Value>*>(keys.data()),
      int(keys.size())
    );

    // atomically install
    _lipp_ptr.store(fresh, std::memory_order_release);
    delete old;
  }

private:
  const size_t                              _flush_threshold;
  DynamicPGM<Key,Searcher,PGMError>        _dpgm;
  std::atomic<LIPP<Key,Value,true>*>        _lipp_ptr;
  mutable std::mutex                        _buffer_mu;
  std::vector<std::pair<Key,Value>>         _buffer;
  std::atomic<bool>                         _shutdown;
};

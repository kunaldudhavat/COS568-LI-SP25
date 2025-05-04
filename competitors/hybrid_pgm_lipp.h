#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <string>

#include "../util.h"               // for KeyValue<>, util::NOT_FOUND
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

// HybridPGMLipp: buffer inserts in DPGM and flush them asynchronously into LIPP.
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
public:
  using KV = KeyValue<KeyType>;

  explicit HybridPGMLipp(const std::vector<int>& params)
    : dp_(params),
      li_(params),
      flush_threshold_( params.empty() ? 100000 : params[0] ),
      stop_flag_(false)
  {
    buffer_.reserve(flush_threshold_);
    // start background flusher
    flush_thread_ = std::thread(&HybridPGMLipp::flush_loop, this);
  }

  ~HybridPGMLipp() override {
    stop_flag_.store(true, std::memory_order_relaxed);
    if (flush_thread_.joinable())
      flush_thread_.join();
    // one final flush
    flush();
  }

  // Build: bulk‐load into LIPP only
  uint64_t Build(const std::vector<KV>& data, size_t num_threads) override {
    uint64_t t = li_.Build(data, num_threads);
    // ensure buffer empty
    buffer_.clear();
    return t;
  }

  // Lookup: first in DPGM, then in LIPP
  size_t EqualityLookup(const KeyType& key, uint32_t thread_id) const override {
    size_t res = dp_.EqualityLookup(key, thread_id);
    if (res != util::NOT_FOUND && res != util::OVERFLOW) return res;
    return li_.EqualityLookup(key, thread_id);
  }

  // Insert: push into DPGM + buffer; background thread will drain buffer
  void Insert(const KV& kv, uint32_t thread_id) override {
    {
      std::lock_guard<std::mutex> lg(buffer_mtx_);
      buffer_.push_back(kv);
    }
    dp_.Insert(kv, thread_id);
  }

  // RangeQuery across both indexes
  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi, uint32_t thread_id) const override {
    uint64_t sum = dp_.RangeQuery(lo, hi, thread_id);
    sum += li_.RangeQuery(lo, hi, thread_id);
    return sum;
  }

  std::string name() const override { return "HybridPGMLipp"; }

  // Approximate memory footprint
  std::size_t size() const override {
    return dp_.size()
         + li_.size()
         + buffer_.size() * sizeof(KV);
  }

private:
  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                              li_;

  std::vector<KV>   buffer_;
  size_t            flush_threshold_;

  std::thread       flush_thread_;
  std::atomic<bool> stop_flag_;
  mutable std::mutex buffer_mtx_;

  // Drain buffer into LIPP, reset DPGM
  void flush() {
    std::vector<KV> tmp;
    {
      std::lock_guard<std::mutex> lg(buffer_mtx_);
      tmp.swap(buffer_);
    }
    if (!tmp.empty()) {
      for (auto &kv : tmp)
        li_.Insert(kv, /*thread_id=*/0);
      // rebuild empty DPGM
      dp_ = DynamicPGM<KeyType, SearchClass, pgm_error>({});
    }
  }

  // Periodically flush every 100 ms
  void flush_loop() {
    while (!stop_flag_.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      flush();
    }
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H
#ifndef TLI_HYBRID_PGM_LIPP_H
#define TLI_HYBRID_PGM_LIPP_H

#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <string>

#include "../benchmark.h"           // for tli::KeyValue
#include "../util.h"
#include "base.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

/// A hybrid index that buffers inserts in DPGM and flushes them
/// into LIPP asynchronously every 100 ms.
template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGMLipp : public Base<KeyType> {
public:
  // Use the common KeyValue type
  using KV = tli::KeyValue<KeyType>;

  HybridPGMLipp(const std::vector<int>& params)
    : dp_(params),
      li_(params),
      flush_threshold_( params.empty() ? 100000 : params[0] ),
      stop_flag_(false)
  {
    // launch background flusher
    flush_thread_ = std::thread(&HybridPGMLipp::flush_loop, this);
  }

  ~HybridPGMLipp() override {
    stop_flag_.store(true, std::memory_order_relaxed);
    if (flush_thread_.joinable())
      flush_thread_.join();
    // final drain
    flush();
  }

  // Bulk-load into LIPP only
  uint64_t Build(const std::vector<KV>& data, size_t num_threads) override {
    uint64_t t = li_.Build(data, num_threads);
    buffer_.reserve(flush_threshold_);
    return t;
  }

  // Check DPGM first, then LIPP
  size_t EqualityLookup(const KeyType& key, uint32_t thread_id) const override {
    size_t v = dp_.EqualityLookup(key, thread_id);
    if (v != util::NOT_FOUND && v != util::OVERFLOW) return v;
    return li_.EqualityLookup(key, thread_id);
  }

  // Always insert into DPGM and buffer
  void Insert(const KV& kv, uint32_t thread_id) override {
    {
      std::lock_guard<std::mutex> lg(buffer_mtx_);
      buffer_.push_back(kv);
    }
    dp_.Insert(kv, thread_id);
  }

  // Range query over both
  uint64_t RangeQuery(const KeyType& lo, const KeyType& hi, uint32_t thread_id) const override {
    uint64_t res = dp_.RangeQuery(lo, hi, thread_id);
    res += li_.RangeQuery(lo, hi, thread_id);
    return res;
  }

  std::string name() const override { return "HybridPGMLipp"; }

  std::size_t size() const override {
    return dp_.size()
         + li_.size()
         + buffer_.size() * sizeof(KV);
  }

private:
  DynamicPGM<KeyType, SearchClass, pgm_error> dp_;
  Lipp<KeyType>                              li_;

  std::vector<KV>    buffer_;
  size_t             flush_threshold_;

  std::thread        flush_thread_;
  std::atomic<bool>  stop_flag_;
  mutable std::mutex buffer_mtx_;

  // Move buffered entries into LIPP and reset DPGM
  void flush() {
    std::vector<KV> tmp;
    {
      std::lock_guard<std::mutex> lg(buffer_mtx_);
      tmp.swap(buffer_);
    }
    if (!tmp.empty()) {
      for (auto &kv : tmp)
        li_.Insert(kv, /*thread_id=*/0);
      dp_ = DynamicPGM<KeyType, SearchClass, pgm_error>({});  // reset model
    }
  }

  // Background flush loop
  void flush_loop() {
    while (!stop_flag_.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      flush();
    }
  }
};

#endif  // TLI_HYBRID_PGM_LIPP_H
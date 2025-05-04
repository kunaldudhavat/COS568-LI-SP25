#pragma once

#include "dynamic_pgm_index.h"
#include "lipp.h"
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>

namespace tli {

// A hybrid that buffers inserts in PGM then flushes into a LIPP tree.
template <typename KeyType, typename SearchClass, uint64_t pgm_error>
class HybridPGMLipp {
public:
  using KeyValue = typename tli::KeyValue<KeyType>;

  HybridPGMLipp() : stop_flag(false) {}
  ~HybridPGMLipp() {
    stop_flag = true;
    if (flush_thread.joinable()) flush_thread.join();
  }

  // Build initial index via DynamicPGM
  uint64_t Build(const std::vector<KeyValue>& data, size_t num_threads) {
    return pgm_.Build(data, num_threads);
  }

  // Buffer into PGM, then trigger background flush
  void Insert(const KeyValue& kv, uint32_t /*thread_id*/) {
    {
      std::lock_guard<std::mutex> guard(buf_mutex_);
      buffer_.push_back(kv);
    }
    maybe_flush_async();
  }

  // First search in PGM, then in buffered LIPP
  KeyValue Lookup(const KeyType& key) {
    auto res = pgm_.Lookup(key);
    if (!res.found) {
      std::lock_guard<std::mutex> guard(buf_mutex_);
      for (auto& kv : buffer_) {
        if (kv.key == key) return KeyValue{kv.key, kv.value, true};
      }
    }
    return res;
  }

private:
  // Pull buffered items into the LIPP tree
  void flush() {
    std::vector<KeyValue> raw;
    {
      std::lock_guard<std::mutex> guard(buf_mutex_);
      raw.swap(buffer_);
    }
    if (!raw.empty()) {
      // *** FIX: use bulk_load, not bulk_load_append ***
      lipp_.bulk_load(raw.data(), raw.size());
    }
  }

  // Launch a background flusher once
  void maybe_flush_async() {
    if (!flusher_started_) {
      flusher_started_ = true;
      flush_thread = std::thread([this]() {
        while (!stop_flag) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          flush();
        }
      });
    }
  }

  DynamicPGMIndex<KeyType, SearchClass, pgm_error> pgm_;
  LIPP<KeyType, KeyType, /*USE_FMCD=*/true>      lipp_;
  std::vector<KeyValue>                         buffer_;
  std::mutex                                    buf_mutex_;
  std::thread                                   flush_thread;
  std::atomic<bool>                             stop_flag;
  bool                                          flusher_started_{false};
};

}  // namespace tli
// File: benchmarks/benchmark_hybrid_pgm_lipp.cc

#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Pareto sweep (threshold sweep)
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool pareto,
                                  const std::vector<int>& params) {
  if (!pareto) {
    // non‐pareto: use exactly the threshold passed in params
    benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 16>>(params);
  } else {
    // full sweep over thresholds
    const std::vector<int> thresholds = {
      50000, 100000, 200000, 500000, 1000000
    };
    for (auto t : thresholds) {
      benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 16>>({t});
    }
  }
}

// Default instantiation for 100M workloads.
// We pick threshold=50K when insert-ratio=0.1 (10% inserts)
// and threshold=1M when insert-ratio=0.9 (90% inserts).
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
  if (filename.find("100M") == std::string::npos) return;

  if (filename.find("0.100000i") != std::string::npos) {
    // 10% insert, 90% lookup ⇒ best observed at 50K
    benchmark.template Run<
      HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>
    >({50000});

  } else if (filename.find("0.900000i") != std::string::npos) {
    // 90% insert, 10% lookup ⇒ best observed at 1M
    benchmark.template Run<
      HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>
    >({1000000});

  } else {
    // fallback default
    benchmark.template Run<
      HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>
    >({200000});
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
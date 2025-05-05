#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Pareto‐sweep for HybridPGMLipp:
//   – We only vary the flush‐threshold at error=16.
//   – Single “sweet‐spot” run uses threshold=2 000 000.
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool pareto,
                                  const std::vector<int>& /*params*/) {
  if (!pareto) {
    // Sweet‐spot: threshold=2M, pgm_error=16
    benchmark.template Run<
      HybridPGMLipp<uint64_t, uint64_t, Searcher, 16>
    >({2000000});
  } else {
    // Sweep thresholds: 50K, 100K, 200K, 500K, 1M, 2M, 5M
    const std::vector<int> thresholds = {
      50000, 100000, 200000, 500000,
      1000000, 2000000, 5000000
    };
    for (auto t : thresholds) {
      benchmark.template Run<
        HybridPGMLipp<uint64_t, uint64_t, Searcher, 16>
      >({t});
    }
  }
}

// Fallback default instantiation for 100M‐key datasets
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
  if (filename.find("100M") != std::string::npos) {
    benchmark.template Run<
      HybridPGMLipp<uint64_t, uint64_t, BranchingBinarySearch<record>, 16>
    >();
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Pareto sweep for HybridPGMLipp
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool pareto,
                                  const std::vector<int>& params) {
  if (!pareto) {
    // Single-threshold run
    benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 16>>(params);
  } else {
    // Sweep thresholds: 50K,100K,200K,500K,1M
    const std::vector<int> thresholds = {50000, 100000, 200000, 500000, 1000000};
    for (auto t : thresholds) {
      benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 16>>({t});
    }
  }
}

// Default run for any 100M dataset
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
  if (filename.find("100M") != std::string::npos) {
    benchmark.template Run<HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>>();
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
// File: benchmarks/benchmark_hybrid_pgm_lipp.cc

#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Pareto sweep (just sweeping thresholds, leave as‐is)
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool pareto,
                                  const std::vector<int>& params) {
  if (!pareto) {
    // non-pareto: use the single threshold passed in params
    benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 16>>(params);
  } else {
    // full sweep
    const std::vector<int> thresholds = {50000, 100000, 200000, 500000, 1000000};
    for (auto t : thresholds) {
      benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 16>>({t});
    }
  }
}

// Default instantiation for any 100M dataset: pick 
//   • 50 K for the 10%-insert mix, 
//   • 1 000 K for the 90%-insert mix.
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
  if (filename.find("100M") == std::string::npos) return;

  // 10%-insert (90% lookup) → threshold = 50 000
  if (filename.find("0.100000i") != std::string::npos) {
    benchmark.template Run<
      HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>
    >({50000});

  // 90%-insert (10% lookup) → threshold = 1 000 000
  } else if (filename.find("0.900000i") != std::string::npos) {
    benchmark.template.Run<
      HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>
    >({1000000});

  // fallback (shouldn’t really happen)
  } else {
    benchmark.template.Run<
      HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>
    >({200000});
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
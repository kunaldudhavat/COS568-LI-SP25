#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Pareto sweep for HybridPGMLipp
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool pareto,
                                  const std::vector<int>& /*params*/) {
  static const std::vector<int> thresholds = {
    50'000, 100'000, 200'000, 500'000,
    1'000'000, 2'000'000, 5'000'000
  };

  if (!pareto) {
    // sweet‐spot threshold = 2 000 000
    benchmark.template Run<
      HybridPGMLipp<uint64_t, SearchClass, 16>
    >({ 2000000 });
  } else {
    for (auto t : thresholds) {
      benchmark.template Run<
        HybridPGMLipp<uint64_t, SearchClass, 16>
      >({ t });
    }
  }
}

// Default run for any 100M dataset
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
  if (filename.find("100M") != std::string::npos) {
    benchmark.template Run<
      HybridPGMLipp<uint64_t, BranchingBinarySearch<record>, 16>
    >();
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
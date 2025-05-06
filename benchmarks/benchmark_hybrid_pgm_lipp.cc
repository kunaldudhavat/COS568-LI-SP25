#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Single fixed–threshold run for HybridPGMLipp
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool /*pareto*/,
                                  const std::vector<int>& /*params*/) {
  // Always use flush_threshold = 1,000,000
  benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 8>>({1000000});
}

// Fallback default instantiation for any 100M dataset
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
  if (filename.find("100M") != std::string::npos) {
    benchmark.template Run<HybridPGMLipp<uint64_t,
                                        BranchingBinarySearch<record>,
                                         8>>({1000000});
  }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
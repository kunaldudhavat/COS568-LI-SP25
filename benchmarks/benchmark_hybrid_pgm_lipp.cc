#include "benchmarks/benchmark_hybrid_pgm_lipp.h"
#include "benchmark.h"
#include "benchmarks/common.h"
#include "competitors/hybrid_pgm_lipp.h"

// Pareto sweep for HybridPGMLipp:
//   – We sweep both rebuild‐threshold and PGM‐error.
//   – “sweet‐spot” run uses threshold=2 000 000, error=16.
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  bool pareto,
                                  const std::vector<int>& /*params*/) {
    // PGM‐error settings to try
    static const std::vector<int> errors = { 8, 16, 32 };
    // Rebuild thresholds to sweep
    static const std::vector<int> thresholds = {
        50'000, 100'000, 200'000, 500'000,
        1'000'000, 2'000'000, 5'000'000
    };

    if (!pareto) {
        // Single “sweet‐spot” run: threshold=2M, error=16
        benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, 16>>({ 2000000 });
    } else {
        // Full Pareto sweep: error × threshold
        for (int e : errors) {
            for (int t : thresholds) {
                benchmark.template Run<HybridPGMLipp<uint64_t, Searcher, e>>({ t });
            }
        }
    }
}

// Fallback default instantiation for non‐pareto 100M datasets.
// Uses BranchingBinarySearch<record> and error=16.
template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
                                  const std::string& filename) {
    if (filename.find("100M") != std::string::npos) {
        benchmark.template Run<HybridPGMLipp<uint64_t,
                                             BranchingBinarySearch<record>,
                                             16>>();
    }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
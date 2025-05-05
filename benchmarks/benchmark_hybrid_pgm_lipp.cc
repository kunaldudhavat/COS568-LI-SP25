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
    static const std::vector<size_t> errors     = { 8, 16, 32 };
    // Rebuild thresholds to sweep
    static const std::vector<size_t> thresholds = {
        50'000, 100'000, 200'000, 500'000,
        1'000'000, 2'000'000, 5'000'000
    };

    if (!pareto) {
        // Single “sweet‐spot” run: threshold=2M, error=16
        benchmark.template Run<
            HybridPGMLipp<uint64_t,   // Key type
                          uint64_t,   // Value type
                          Searcher,   // Search strategy
                          16          // PGM‐error
                         >
        >({ 2000000 });
    } else {
        // Full Pareto sweep: error × threshold
        for (auto e : errors) {
            for (auto t : thresholds) {
                benchmark.template Run<
                    HybridPGMLipp<uint64_t,uint64_t,Searcher,e>
                >({ int(t) });
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
        benchmark.template Run<
            HybridPGMLipp<uint64_t,            // Key type
                          uint64_t,            // Value type
                          BranchingBinarySearch<record>,
                          16                  // PGM‐error
                         >
        >();
    }
}

INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
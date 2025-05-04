// #pragma once

// #include <string>
// #include <vector>

// #include "benchmark.h"
// #include "benchmarks/common.h"
// #include "competitors/hybrid_pgm_lipp.h"

// /**
//  * Forward declarations for the HybridPGMLipp benchmark:
//  *
//  * - The first overload is used when --pareto is specified, taking
//  *   a vector<int> of params (where params[0] is flush threshold).
//  * - The second overload is the filename‐based run, invoked when --
//  *   only=<IndexName> without pareto; it triggers a single run for
//  *   any dataset whose name contains "100M".
//  */

// // Pareto‐style run: sweep multiple thresholds in params
// template <typename Searcher>
// void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
//                                   bool pareto,
//                                   const std::vector<int>& params);

// // Filename‐based run: no pareto, pick the default flush threshold
// template <int record>
// void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark,
//                                   const std::string& filename);

// // Instantiate for multi-thread support (0,1,2 error‐tracking modes)
// INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);

#pragma once
#include "benchmark.h"

// same prototypes as before:
template <typename Searcher>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                  bool pareto,
                                  const std::vector<int>& params);

template <int record>
void benchmark_64_hybrid_pgm_lipp(tli::Benchmark<uint64_t>& benchmark, 
                                  const std::string& filename);

// this line is new/needed so the linker sees your overloads
INSTANTIATE_TEMPLATES_MULTITHREAD(benchmark_64_hybrid_pgm_lipp, uint64_t);
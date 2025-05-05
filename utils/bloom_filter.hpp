#pragma once

#include <vector>
#include <functional>
#include <cmath>
#include <cstddef>

namespace util {

/// Very simple double‐hash Bloom filter.
/// T must be hashable with std::hash<T>
template<typename T>
class BloomFilter {
public:
  BloomFilter(size_t expected_items, double fp_rate = 0.01) {
    // m = −(n ln p) / (ln 2)^2
    double m_d = - (double)expected_items * std::log(fp_rate)
                 / (std::log(2.0) * std::log(2.0));
    m_bits   = size_t(std::ceil(m_d));
    // k = (m/n) ln 2
    double k_d = (m_d / (double)expected_items) * std::log(2.0);
    k_hashes = size_t(std::ceil(k_d));
    bits.resize(m_bits);
  }

  void add(const T &x) {
    auto h1 = hasher1(x);
    auto h2 = hasher2(x);
    for (size_t i = 0; i < k_hashes; ++i) {
      bits[(h1 + i*h2) % m_bits] = true;
    }
  }

  bool contains(const T &x) const {
    auto h1 = hasher1(x);
    auto h2 = hasher2(x);
    for (size_t i = 0; i < k_hashes; ++i) {
      if (!bits[(h1 + i*h2) % m_bits]) return false;
    }
    return true;
  }

  void clear() {
    std::fill(bits.begin(), bits.end(), false);
  }

private:
  std::vector<bool>          bits;
  size_t                     m_bits;
  size_t                     k_hashes;
  std::hash<T>               hasher1;
  std::function<size_t(const T&)> hasher2 =
    [](const T &x){
      auto h = std::hash<T>{}(x);
      // a cheap xor‐mix
      return (h ^ (h >> 33) ^ 0x9e3779b97f4a7c15ULL);
    };
};

} // namespace util
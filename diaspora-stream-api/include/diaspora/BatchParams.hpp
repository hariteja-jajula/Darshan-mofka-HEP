/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_BATCH_PARAMS_HPP
#define DIASPORA_API_BATCH_PARAMS_HPP

#include <diaspora/ForwardDcl.hpp>
#include <cstdint>
#include <limits>

namespace diaspora {

/**
 * @brief Strongly typped size_t meant to store the batch size to
 * use when creating a Producer.
 */
struct BatchSize {

    std::size_t value;

    explicit constexpr BatchSize(std::size_t val)
    : value(val) {}

    /**
     * @brief Returns a value telling the producer to try its best
     * to adapt the batch size to the use-case and workload.
     */
    static inline BatchSize Adaptive() {
        return BatchSize{std::numeric_limits<std::size_t>::max()};
    }

    inline bool operator<(const BatchSize& other) const { return value < other.value; } // LCOV_EXCL_LINE
    inline bool operator>(const BatchSize& other) const { return value > other.value; } // LCOV_EXCL_LINE
    inline bool operator<=(const BatchSize& other) const { return value <= other.value; } // LCOV_EXCL_LINE
    inline bool operator>=(const BatchSize& other) const { return value >= other.value; } // LCOV_EXCL_LINE
    inline bool operator==(const BatchSize& other) const { return value == other.value; } // LCOV_EXCL_LINE
    inline bool operator!=(const BatchSize& other) const { return value != other.value; } // LCOV_EXCL_LINE
};

/**
 * @brief Strongly typped size_t meant to store the maximum number of batches to
 * use at any time in a producer or a consumer.
 */
struct MaxNumBatches {

    std::size_t value;

    explicit constexpr MaxNumBatches(std::size_t val)
    : value(val) {}

    static MaxNumBatches Infinity() {
        return MaxNumBatches{std::numeric_limits<std::size_t>::max()};
    }

    inline bool operator<(const MaxNumBatches& other) const { return value < other.value; } // LCOV_EXCL_LINE
    inline bool operator>(const MaxNumBatches& other) const { return value > other.value; } // LCOV_EXCL_LINE
    inline bool operator<=(const MaxNumBatches& other) const { return value <= other.value; } // LCOV_EXCL_LINE
    inline bool operator>=(const MaxNumBatches& other) const { return value >= other.value; } // LCOV_EXCL_LINE
    inline bool operator==(const MaxNumBatches& other) const { return value == other.value; } // LCOV_EXCL_LINE
    inline bool operator!=(const MaxNumBatches& other) const { return value != other.value; } // LCOV_EXCL_LINE
};

}

#endif

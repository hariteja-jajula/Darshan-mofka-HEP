/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_NUM_EVENTS_HPP
#define DIASPORA_API_NUM_EVENTS_HPP

#include <diaspora/ForwardDcl.hpp>

#include <cstdint>

namespace diaspora {

/**
 * @brief Strongly typped size_t meant to store a number of events.
 */
struct NumEvents {

    std::size_t value;

    explicit constexpr NumEvents(std::size_t val)
    : value(val) {}

    /**
     * @brief A value so large you are unlikely to see than many events
     * in the lifetime of the application.
     */
    static NumEvents Infinity();

    inline bool operator<(const NumEvents& other) const { return value < other.value; } // LCOV_EXCL_LINE
    inline bool operator>(const NumEvents& other) const { return value > other.value; } // LCOV_EXCL_LINE
    inline bool operator<=(const NumEvents& other) const { return value <= other.value; } // LCOV_EXCL_LINE
    inline bool operator>=(const NumEvents& other) const { return value >= other.value; } // LCOV_EXCL_LINE
    inline bool operator==(const NumEvents& other) const { return value == other.value; } // LCOV_EXCL_LINE
    inline bool operator!=(const NumEvents& other) const { return value != other.value; } // LCOV_EXCL_LINE
};

}

#endif

/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_EVENT_ID_HPP
#define DIASPORA_EVENT_ID_HPP

#include <diaspora/ForwardDcl.hpp>

#include <limits>
#include <cstdint>

namespace diaspora {

using EventID = std::uint64_t;

constexpr const EventID NoMoreEvents = std::numeric_limits<EventID>::max();

}

#endif

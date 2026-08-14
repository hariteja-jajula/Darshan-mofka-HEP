/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_EVENT_PROCESSOR_HPP
#define DIASPORA_EVENT_PROCESSOR_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Event.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace diaspora {

/**
 * @brief Throw this from an EventProcessor function to tell
 * the consumer to stop feeding the EventProcessor and get out
 * of the process function.
 */
struct StopEventProcessor {};

using EventProcessor = std::function<void(const std::optional<Event>& event)>;

}

#endif

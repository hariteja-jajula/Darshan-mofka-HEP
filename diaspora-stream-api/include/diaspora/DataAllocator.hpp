/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_DATA_ALLOCATOR_HPP
#define DIASPORA_API_DATA_ALLOCATOR_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/DataView.hpp>
#include <diaspora/DataDescriptor.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace diaspora {

/**
 * @brief DataAllocator is the type of a function that takes the
 * Metadata of an event as well as the DataDescriptor of the associated
 * data, and returns a Data object indicating where in memory the data
 * of the event should be placed by the Consumer.
 */
using DataAllocator = std::function<DataView(const Metadata&, const DataDescriptor&)>;

}

#endif

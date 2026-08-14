/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_DATA_SELECTOR_HPP
#define DIASPORA_API_DATA_SELECTOR_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/DataDescriptor.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace diaspora {

/**
 * @brief The DataSelector is a function passed to a Consumer
 * and whose role is to tell it if the Data piece associated with
 * an event should be pulled from the Diaspora server, and if so
 * which part of this data, by returning a DataDescriptor instance
 * from the provided DataDescriptor.
 *
 * An easy way to select the whole data is to just return the provided
 * DataDescriptor. An easy way to decline all the data is to just return
 * DataDescriptor{}.
 */
using DataSelector = std::function<DataDescriptor(const Metadata&, const DataDescriptor&)>;

}

#endif

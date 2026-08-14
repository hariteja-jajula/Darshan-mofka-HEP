/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_INVALID_METADATA_HPP
#define DIASPORA_API_INVALID_METADATA_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Exception.hpp>

#include <exception>
#include <stdexcept>

namespace diaspora {

/**
 * @brief The InvalidMetadata class is the exception raised
 * when a Metadata is not valid.
 */
class InvalidMetadata : public Exception {

    public:

    template<typename ... Args>
    InvalidMetadata(Args&&... args)
    : Exception(std::forward<Args>(args)...) {}
};

}

#endif

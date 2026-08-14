/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_EXCEPTION_HPP
#define DIASPORA_API_EXCEPTION_HPP

#include <diaspora/ForwardDcl.hpp>

#include <exception>
#include <stdexcept>

namespace diaspora {

/**
 * @brief Exception class to use in Diaspora backend implementations.
 */
class Exception : public std::logic_error {

    public:

    Exception(const Exception&) = default; // LCOV_EXCL_LINE

    Exception(Exception&&) = default; // LCOV_EXCL_LINE

    Exception& operator=(const Exception&) = default; // LCOV_EXCL_LINE

    Exception& operator=(Exception&&) = default; // LCOV_EXCL_LINE

    Exception(const char* w)
    : std::logic_error(w) {}

    Exception(const std::string& w)
    : std::logic_error(w) {}
};

}

#endif

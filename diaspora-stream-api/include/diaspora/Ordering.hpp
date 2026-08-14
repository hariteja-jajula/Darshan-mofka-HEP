/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_ORDERING_HPP
#define DIASPORA_API_ORDERING_HPP

#include <diaspora/ForwardDcl.hpp>

namespace diaspora {

enum class Ordering : bool {
    Strict = true, /* events must be stored in the same order they are produced */
    Loose  = false /* events can be stored in a different order */
};

}

#endif

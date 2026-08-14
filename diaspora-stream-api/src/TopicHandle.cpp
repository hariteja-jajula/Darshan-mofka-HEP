/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "diaspora/Exception.hpp"
#include "diaspora/TopicHandle.hpp"
#include "diaspora/Driver.hpp"
#include "PimplUtil.hpp"
#include <limits>

namespace diaspora {

Driver TopicHandle::driver() const {
    return Driver(self->driver());
}

}

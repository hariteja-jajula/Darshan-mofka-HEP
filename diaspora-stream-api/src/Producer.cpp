/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "diaspora/Producer.hpp"
#include "diaspora/Exception.hpp"
#include "diaspora/TopicHandle.hpp"
#include "diaspora/Future.hpp"
#include "PimplUtil.hpp"
#include <limits>

namespace diaspora {

TopicHandle Producer::topic() const {
    return TopicHandle(self->topic());
}

}

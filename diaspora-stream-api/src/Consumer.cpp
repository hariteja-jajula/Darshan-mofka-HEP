/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "diaspora/Consumer.hpp"
#include "diaspora/Exception.hpp"
#include "diaspora/TopicHandle.hpp"
#include "diaspora/Future.hpp"

#include <limits>

using namespace std::string_literals;

namespace diaspora {

Consumer::~Consumer() {
    if(self.use_count() == 1)
        self->unsubscribe();
}

TopicHandle Consumer::topic() const {
    return self->topic();
}

NumEvents NumEvents::Infinity() {
    return NumEvents{std::numeric_limits<size_t>::max()};
}

}

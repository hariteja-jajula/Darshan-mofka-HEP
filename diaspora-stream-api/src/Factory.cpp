/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <diaspora/Factory.hpp>
#include <diaspora/Exception.hpp>
#include <dlfcn.h>
#include <unordered_map>
#include <functional>
#include <memory>
#include <vector>

namespace diaspora {

DIASPORA_INSTANTIATE_FACTORY(DriverInterface, const Metadata&);
DIASPORA_INSTANTIATE_FACTORY(ValidatorInterface, const Metadata&);
DIASPORA_INSTANTIATE_FACTORY(SerializerInterface, const Metadata&);
DIASPORA_INSTANTIATE_FACTORY(PartitionSelectorInterface, const Metadata&);

}

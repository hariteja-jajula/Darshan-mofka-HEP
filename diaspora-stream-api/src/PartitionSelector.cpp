/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "diaspora/Exception.hpp"
#include "diaspora/PartitionSelector.hpp"
#include "PimplUtil.hpp"
#include "DefaultPartitionSelector.hpp"
#include <unordered_map>

namespace diaspora {

PartitionSelector::PartitionSelector()
: self(std::make_shared<DefaultPartitionSelector>()) {}

DIASPORA_REGISTER_PARTITION_SELECTOR(diaspora, default, DefaultPartitionSelector);

PartitionSelector PartitionSelector::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    if(!json.contains("type")) {
        return PartitionSelector{};
    }
    auto& type = json["type"];
    if(!type.is_string()) {
        throw Exception(
            "Cannot create PartitionSelector from Metadata: "
            "invalid \"type\" field in Metadata (expected string)");
    }
    auto& type_str = type.get_ref<const std::string&>();
    std::shared_ptr<PartitionSelectorInterface> ts = PartitionSelectorFactory::create(type_str, metadata);
    return ts;
}

}

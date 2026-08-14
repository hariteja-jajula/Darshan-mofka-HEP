/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonUtil.hpp"
#include "diaspora/Exception.hpp"
#include "diaspora/Validator.hpp"
#include "PimplUtil.hpp"
#include "DefaultValidator.hpp"
#include "SchemaValidator.hpp"
#include <unordered_map>

namespace diaspora {

Validator::Validator()
: self(std::make_shared<DefaultValidator>()) {}

DIASPORA_REGISTER_VALIDATOR(diaspora, default, DefaultValidator);
DIASPORA_REGISTER_VALIDATOR(diaspora, schema, SchemaValidator);

Validator Validator::FromMetadata(const Metadata& metadata) {
    auto& json = metadata.json();
    if(!json.is_object()) {
        throw Exception(
            "Cannot create Validator from Metadata: "
            "invalid Metadata (expected JSON object)");
    }
    if(!json.contains("type")) {
        return Validator{};
    }
    auto& type = json["type"];
    if(!type.is_string()) {
        throw Exception(
            "Cannot create Validator from Metadata: "
            "invalid \"type\" field in Metadata (expected string)");
    }
    auto& type_str = type.get_ref<const std::string&>();
    std::shared_ptr<ValidatorInterface> v = ValidatorFactory::create(type_str, metadata);
    return v;
}

}

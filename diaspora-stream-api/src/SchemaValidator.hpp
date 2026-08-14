/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_SCHEMA_VALIDATOR_H
#define DIASPORA_API_SCHEMA_VALIDATOR_H

#include "JsonUtil.hpp"
#include "diaspora/Metadata.hpp"
#include "diaspora/Validator.hpp"
#include "diaspora/Json.hpp"

namespace diaspora {

class SchemaValidator : public ValidatorInterface {

    using json = nlohmann::json;

    json                m_json_schema;
    JsonSchemaValidator m_json_validator;

    public:

    SchemaValidator(json schema)
    : m_json_schema(std::move(schema))
    , m_json_validator(m_json_schema) {}

    void validate(const Metadata& metadata, const DataView& data) const override {
        (void)data;
        auto errors = m_json_validator.validate(metadata.json());
        if(!errors.empty()) {
            std::stringstream ss;
            ss << "Metadata does not comply to required schema:\n";
            for(size_t i=0; i < errors.size()-1; ++i) {
                ss << errors[i];
            }
            ss << errors[errors.size()-1];
            throw InvalidMetadata{ss.str()};
        }
    }

    Metadata metadata() const override {
        auto config = json::object();
        config["type"] = "schema";
        config["schema"] = m_json_schema;
        return Metadata{std::move(config)};
    }

    static std::shared_ptr<ValidatorInterface> create(const Metadata& metadata) {
        if(!metadata.json().is_object())
            throw Exception{"Provided Metadata is not a JSON object"};
        if(!metadata.json().contains("schema"))
            throw Exception{"SchemaValidator is expecting a \"schema\" entry in its configuration"};
        return std::make_shared<SchemaValidator>(metadata.json()["schema"]);
    }

};

}

#endif

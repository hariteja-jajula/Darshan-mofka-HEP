/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_EVENTBRIDGE_VALIDATOR_H
#define DIASPORA_EVENTBRIDGE_VALIDATOR_H

#include "JsonUtil.hpp"
#include "diaspora/Metadata.hpp"
#include "diaspora/Validator.hpp"
#include "diaspora/Json.hpp"

namespace diaspora {

class EventbridgeValidator : public ValidatorInterface {

    using json = nlohmann::json;

    json                             m_schema;
    std::function<bool(const json&)> m_validator;

    public:

    EventbridgeValidator(json schema, std::function<bool(const json&)> func)
    : m_schema(std::move(schema))
    , m_validator(std::move(func)) {}

    void validate(const Metadata& metadata, const DataView& data) const override;

    Metadata metadata() const override;

    static std::shared_ptr<ValidatorInterface> create(const Metadata& metadata);

};

}

#endif

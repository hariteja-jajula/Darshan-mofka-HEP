/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_DEFAULT_VALIDATOR_H
#define DIASPORA_API_DEFAULT_VALIDATOR_H

#include "JsonUtil.hpp"
#include "diaspora/Metadata.hpp"
#include "diaspora/Validator.hpp"
#include "diaspora/Json.hpp"

namespace diaspora {

class DefaultValidator : public ValidatorInterface {

    using json = nlohmann::json;

    public:

    void validate(const Metadata& metadata, const DataView& data) const override {
        (void)metadata;
        (void)data;
    }

    Metadata metadata() const override {
        return Metadata{"{\"type\":\"default\"}"_json};
    }

    static std::shared_ptr<ValidatorInterface> create(const Metadata& metadata) {
        (void)metadata;
        return std::make_shared<DefaultValidator>();
    }

};

}

#endif

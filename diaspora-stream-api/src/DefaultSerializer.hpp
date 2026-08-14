/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_DEFAULT_SERIALIZER_H
#define DIASPORA_DEFAULT_SERIALIZER_H

#include "JsonUtil.hpp"
#include "diaspora/Serializer.hpp"
#include "diaspora/Json.hpp"

namespace diaspora {

class DefaultSerializer : public SerializerInterface {

    public:

    void serialize(Archive& archive, const Metadata& metadata) const override {
        const auto str = metadata.dump();
        size_t s = str.size();
        archive.write(&s, sizeof(s));
        archive.write(str.data(), s);
    }

    void deserialize(Archive& archive, Metadata& metadata) const override {
        size_t s = 0;
        archive.read(&s, sizeof(s));
        std::string str;
        str.resize(s);
        archive.read(const_cast<char*>(str.data()), s);
        try {
            metadata.json() = nlohmann::json::parse(str);
        } catch(const std::exception& ex) {
            throw Exception{std::string{"Could not deserialize Serializer metadata: "} + ex.what()};
        }
    }

    Metadata metadata() const override {
        return Metadata{"{\"type\":\"default\"}"};
    }

    static std::shared_ptr<SerializerInterface> create(const Metadata& metadata) {
        (void)metadata;
        return std::make_shared<DefaultSerializer>();
    }
};

}

#endif

/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_METADATA_HPP
#define DIASPORA_API_METADATA_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Json.hpp>

#include <string>
#include <string_view>
#include <memory>
#include <vector>

namespace diaspora {

/**
 * @brief A Metadata is an object that encapsulates the metadata of an event.
 * The Metadata class encapsulates a json object. If the metadata is not JSON,
 * it can simply be used as a string.
 */
class Metadata {

    public:

    /**
     * @brief Constructor taking a string. The string will be moved
     * into the Metadata object, hence it is passed by value.
     *
     * @param content string content.
     * @param parse Whether to parse the string into a JSON object. If false,
     *              the content will be assigned as a string instead.
     */
    Metadata(const std::string& content = "{}", bool parse = true)
    : Metadata(std::string_view{content}, parse) {}

    Metadata(std::string_view content, bool parse = true) {
        if(parse) {
            try {
                m_content = nlohmann::json::parse(content);
            } catch(const std::exception& ex) {
                throw Exception{std::string{"Could not parse JSON: "} + ex.what()};
            }
        } else {
            m_content = content;
        }
    }

    Metadata(const char* content, bool parse = true)
    : Metadata(std::string_view{content}, parse) {}

    /**
     * @brief Constructor taking an already formed JSON document.
     *
     * @param json
     */
    Metadata(nlohmann::json json)
    : m_content(std::move(json)) {}

    /**
     * @brief Copy-constructor.
     */
    Metadata(const Metadata&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    Metadata(Metadata&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    Metadata& operator=(const Metadata&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    Metadata& operator=(Metadata&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    ~Metadata() = default; // LCOV_EXCL_LINE

    /**
     * @brief Returns the underlying JSON document.
     */
    const nlohmann::json& json() const & {
        return m_content;
    }

    /**
     * @brief Returns the underlying JSON document.
     */
    nlohmann::json& json() & {
        return m_content;
    }

    /**
     * @brief Returns the underlying JSON document.
     */
    nlohmann::json&& json() && {
        return std::move(m_content);
    }

    /**
     * @brief Returns the underlying string representation
     * of the Metadata.
     *
     * Note: if the Metadata has been constructed from a JSON document,
     * this function will trigger its serialization into a string.
     */
    template<typename ... Args>
    auto dump(Args&&... args) const {
        return m_content.dump(std::forward<Args>(args)...);
    }

    /**
     * @brief Returns the Metadata's string if the underlying JSON is a string.
     */
    const auto& string() const {
        if(m_content.is_string())
            return m_content.get_ref<const std::string&>();
        else
            throw diaspora::Exception{"Metadata content is not a string"};
    }

    /**
     * @brief Returns the Metadata's string if the underlying JSON is a string.
     */
    auto& string() {
        if(m_content.is_string())
            return m_content.get_ref<std::string&>();
        else
            throw diaspora::Exception{"Metadata content is not a string"};
    }

    private:

    nlohmann::json m_content;
};

}

#endif

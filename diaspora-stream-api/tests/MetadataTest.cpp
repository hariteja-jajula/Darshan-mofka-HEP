/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <diaspora/Metadata.hpp>

TEST_CASE("Diaspora API Metadata test", "[metadata]") {

    SECTION("Default Metadata object") {

        auto md = diaspora::Metadata{};
        REQUIRE(md.json().is_object());
        REQUIRE(md.dump() == "{}");

        SECTION("Modify metadata using string accessor") {
            md = R"({"x":1,"y":2.3})";
            REQUIRE(md.json().is_object());
            REQUIRE(((const diaspora::Metadata&)md).json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.dump() == R"({"x":1,"y":2.3})");
        }

        SECTION("Modify metadata using json accessor") {
            md.json()["x"] = 1;
            md.json()["y"] = 2.3;
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.dump() == R"({"x":1,"y":2.3})");
        }
    }

    SECTION("Constructors from strings without parsing") {

        SECTION("string constructor") {
            std::string content{"abcd"};
            auto md = diaspora::Metadata{content, false};
            REQUIRE(!md.json().is_object());
            REQUIRE(md.json().is_string());
            REQUIRE(md.dump() == R"("abcd")");
            REQUIRE(md.string() == R"(abcd)");
        }

        SECTION("string_view constructor") {
            std::string_view content{"abcd"};
            auto md = diaspora::Metadata{content, false};
            REQUIRE(!md.json().is_object());
            REQUIRE(md.json().is_string());
            REQUIRE(md.dump() == R"("abcd")");
            REQUIRE(md.string() == R"(abcd)");
        }

        SECTION("const char* constructor") {
            const char* content = "abcd";
            auto md = diaspora::Metadata{content, false};
            REQUIRE(!md.json().is_object());
            REQUIRE(md.json().is_string());
            REQUIRE(md.dump() == R"("abcd")");
            REQUIRE(md.string() == R"(abcd)");
        }

    }

    SECTION("Constructors from strings with parsing") {

        SECTION("string constructor") {
            std::string content{R"({"x":1,"y":2.3})"};
            auto md = diaspora::Metadata{content, true};
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.dump() == R"({"x":1,"y":2.3})");
        }

        SECTION("string_view constructor") {
            std::string_view content{R"({"x":1,"y":2.3})"};
            auto md = diaspora::Metadata{content, true};
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.dump() == R"({"x":1,"y":2.3})");
        }

        SECTION("const char* constructor") {
            const char* content = R"({"x":1,"y":2.3})";
            auto md = diaspora::Metadata{content, true};
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.dump() == R"({"x":1,"y":2.3})");
        }

        SECTION("string constructor (invalid JSON)") {
            std::string content{R"({"x":1,"y":)"};
            REQUIRE_THROWS_AS(diaspora::Metadata(content, true), diaspora::Exception);
        }

        SECTION("string_view constructor (invalid JSON)") {
            std::string_view content{R"({"x":1,"y":)"};
            REQUIRE_THROWS_AS(diaspora::Metadata(content, true), diaspora::Exception);
        }

        SECTION("const char* constructor (invalid JSON)") {
            const char* content = R"({"x":1,"y":)";
            REQUIRE_THROWS_AS(diaspora::Metadata(content, true), diaspora::Exception);
        }

    }

    SECTION("JSON constructor") {
        auto content = nlohmann::json::parse(R"({"x":1,"y":2.3})");
        auto md = diaspora::Metadata{content};
        REQUIRE(md.json().is_object());
        REQUIRE(md.json().contains("x"));
        REQUIRE(md.json().contains("y"));
        REQUIRE(md.dump() == R"({"x":1,"y":2.3})");
        REQUIRE_THROWS_AS(md.string(), diaspora::Exception);
    }

}

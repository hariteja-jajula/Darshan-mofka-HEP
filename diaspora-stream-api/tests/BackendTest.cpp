/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <cstdlib>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include "SimpleBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, SimpleDriver);

TEST_CASE("Diaspora API test", "[diaspora-api]") {

    SECTION("Create invalid Driver") {

        REQUIRE_THROWS_AS(
            diaspora::DriverFactory::create("unknown", {}),
            diaspora::Exception);

        REQUIRE_THROWS_AS(
            diaspora::DriverFactory::create("unknown:libunknown.so", {}),
            diaspora::Exception);

    }

    SECTION("Create Driver") {

        const char* backend = std::getenv("DIASPORA_TEST_BACKEND");
        const char* args    = std::getenv("DIASPORA_TEST_BACKEND_ARGS");
        backend = backend ? backend : "simple";
        args    = args ? args : "{}";

        auto driver = diaspora::DriverFactory::create(backend, diaspora::Metadata{args});
        REQUIRE(static_cast<bool>(driver));

    }
}

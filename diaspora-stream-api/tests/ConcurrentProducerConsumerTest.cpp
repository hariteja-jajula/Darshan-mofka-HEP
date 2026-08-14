/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <cstdlib>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include "SimpleBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, SimpleDriver);

static int topic_num = 0;

TEST_CASE("Concurrent producer/consumer", "[concurrent]") {

    const char* backend      = std::getenv("DIASPORA_TEST_BACKEND");
    const char* backend_args = std::getenv("DIASPORA_TEST_BACKEND_ARGS");
    const char* topic_args   = std::getenv("DIASPORA_TEST_TOPIC_ARGS");
    backend                  = backend ? backend : "simple";
    backend_args             = backend_args ? backend_args : "{}";
    topic_args               = topic_args ? topic_args : "{}";

    diaspora::Metadata options{backend_args};
    diaspora::Driver driver = diaspora::Driver::New(backend, options);
    REQUIRE(static_cast<bool>(driver));

    SECTION("Single producer and consumer simultaneously") {
        std::string topic_name = "concurrent_topic_" + std::to_string(topic_num++);
        REQUIRE_NOTHROW(driver.createTopic(topic_name, diaspora::Metadata{topic_args}));
        auto topic = driver.openTopic(topic_name);
        REQUIRE(static_cast<bool>(topic));

        const unsigned num_events = 1000;
        std::atomic<unsigned> events_produced{0};
        std::atomic<unsigned> events_consumed{0};
        std::atomic<bool> producer_done{false};
        std::mutex error_mutex;
        std::string error_message;

        // Producer thread
        std::thread producer_thread([&]() {
            try {
                auto producer = topic.producer("producer", driver.defaultThreadPool());
                for (unsigned i = 0; i < num_events; ++i) {
                    diaspora::Metadata metadata{
                        std::string{"{\"index\":"} + std::to_string(i) + "}"
                    };
                    auto future = producer.push(metadata, diaspora::DataView{});
                    future.wait(1000);
                    events_produced.fetch_add(1);
                }
                producer.flush().wait(5000);
                producer_done = true;
            } catch (const std::exception& ex) {
                std::lock_guard<std::mutex> lock(error_mutex);
                error_message = std::string("Producer error: ") + ex.what();
            }
        });

        // Consumer thread
        std::thread consumer_thread([&]() {
            try {
                auto consumer = topic.consumer("consumer", driver.defaultThreadPool());
                auto start = std::chrono::steady_clock::now();
                auto timeout = std::chrono::seconds(60);

                // Keep the future alive across iterations. If wait(100) times out the
                // Promise still holds a weak_ptr to the State: destroying the Future
                // before it resolves drops the only shared_ptr, making setValue() a
                // no-op and silently losing the event.
                diaspora::Future<std::optional<diaspora::Event>> pending_future;

                while (events_consumed < num_events) {
                    if (std::chrono::steady_clock::now() - start > timeout) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        if (error_message.empty())
                            error_message = "Consumer timeout: consumed "
                                + std::to_string(events_consumed.load()) + "/"
                                + std::to_string(num_events) + " events";
                        break;
                    }
                    if (!pending_future) pending_future = consumer.pull();
                    auto event_opt = pending_future.wait(100);
                    if (event_opt) {
                        pending_future = {};
                        if (event_opt->id() != diaspora::NoMoreEvents) {
                            events_consumed.fetch_add(1);
                            event_opt->acknowledge();
                        }
                    }
                }
            } catch (const std::exception& ex) {
                std::lock_guard<std::mutex> lock(error_mutex);
                if (error_message.empty())
                    error_message = std::string("Consumer error: ") + ex.what();
            }
        });

        producer_thread.join();
        consumer_thread.join();

        {
            std::lock_guard<std::mutex> lock(error_mutex);
            if (!error_message.empty()) {
                FAIL(error_message);
            }
        }

        REQUIRE(events_produced == num_events);
        REQUIRE(events_consumed == num_events);
    }

    SECTION("Producer faster than consumer") {
        std::string topic_name = "concurrent_topic_" + std::to_string(topic_num++);
        REQUIRE_NOTHROW(driver.createTopic(topic_name, diaspora::Metadata{topic_args}));
        auto topic = driver.openTopic(topic_name);
        REQUIRE(static_cast<bool>(topic));

        const unsigned num_events = 30;
        std::atomic<unsigned> events_produced{0};
        std::atomic<unsigned> events_consumed{0};
        std::atomic<bool> producer_done{false};
        std::mutex error_mutex;
        std::string error_message;

        // Producer thread (fast - no delay)
        std::thread producer_thread([&]() {
            try {
                auto producer = topic.producer("producer", driver.defaultThreadPool());
                for (unsigned i = 0; i < num_events; ++i) {
                    diaspora::Metadata metadata{
                        std::string{"{\"index\":"} + std::to_string(i) + "}"
                    };
                    std::string data = "Event data " + std::to_string(i);
                    auto future = producer.push(
                        metadata,
                        diaspora::DataView{data.data(), data.size()});
                    future.wait(1000);
                    events_produced.fetch_add(1);
                }
                producer.flush().wait(5000);
                producer_done = true;
            } catch (const std::exception& ex) {
                std::lock_guard<std::mutex> lock(error_mutex);
                error_message = std::string("Producer error: ") + ex.what();
            }
        });

        // Consumer thread (slow - with delay)
        std::thread consumer_thread([&]() {
            try {
                auto consumer = topic.consumer("consumer", driver.defaultThreadPool());
                auto start = std::chrono::steady_clock::now();
                auto timeout = std::chrono::seconds(60);

                diaspora::Future<std::optional<diaspora::Event>> pending_future;

                while (events_consumed < num_events) {
                    if (std::chrono::steady_clock::now() - start > timeout) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        if (error_message.empty())
                            error_message = "Consumer timeout: consumed "
                                + std::to_string(events_consumed.load()) + "/"
                                + std::to_string(num_events) + " events";
                        break;
                    }
                    if (!pending_future) pending_future = consumer.pull();
                    auto event_opt = pending_future.wait(100);
                    if (event_opt) {
                        pending_future = {};
                        if (event_opt->id() != diaspora::NoMoreEvents) {
                            events_consumed.fetch_add(1);
                            event_opt->acknowledge();
                            // Simulate slow consumer
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        }
                    }
                }
            } catch (const std::exception& ex) {
                std::lock_guard<std::mutex> lock(error_mutex);
                if (error_message.empty())
                    error_message = std::string("Consumer error: ") + ex.what();
            }
        });

        producer_thread.join();
        consumer_thread.join();

        {
            std::lock_guard<std::mutex> lock(error_mutex);
            if (!error_message.empty()) {
                FAIL(error_message);
            }
        }

        REQUIRE(events_produced == num_events);
        REQUIRE(events_consumed == num_events);
    }

    SECTION("Multiple producers single consumer") {
        std::string topic_name = "concurrent_topic_" + std::to_string(topic_num++);
        REQUIRE_NOTHROW(driver.createTopic(topic_name, diaspora::Metadata{topic_args}));
        auto topic = driver.openTopic(topic_name);
        REQUIRE(static_cast<bool>(topic));

        const unsigned num_producers = 3;
        const unsigned events_per_producer = 20;
        const unsigned total_events = num_producers * events_per_producer;
        std::atomic<unsigned> events_produced{0};
        std::atomic<unsigned> events_consumed{0};
        std::atomic<unsigned> producers_done{0};
        std::mutex error_mutex;
        std::string error_message;

        // Multiple producer threads
        std::vector<std::thread> producer_threads;
        for (unsigned p = 0; p < num_producers; ++p) {
            producer_threads.emplace_back([&, p]() {
                try {
                    auto producer = topic.producer(
                        "producer_" + std::to_string(p),
                        driver.defaultThreadPool());
                    for (unsigned i = 0; i < events_per_producer; ++i) {
                        diaspora::Metadata metadata{
                            std::string{"{\"producer\":"} + std::to_string(p) +
                            ",\"index\":" + std::to_string(i) + "}"
                        };
                        auto future = producer.push(metadata, diaspora::DataView{});
                        future.wait(1000);
                        events_produced.fetch_add(1);
                    }
                    producer.flush().wait(5000);
                    producers_done.fetch_add(1);
                } catch (const std::exception& ex) {
                    std::lock_guard<std::mutex> lock(error_mutex);
                    if (error_message.empty()) {
                        error_message = std::string("Producer ") + std::to_string(p) +
                                       " error: " + ex.what();
                    }
                }
            });
        }

        // Consumer thread
        std::thread consumer_thread([&]() {
            try {
                auto consumer = topic.consumer("consumer", driver.defaultThreadPool());
                auto start = std::chrono::steady_clock::now();
                auto timeout = std::chrono::seconds(60);

                diaspora::Future<std::optional<diaspora::Event>> pending_future;

                while (events_consumed < total_events) {
                    if (std::chrono::steady_clock::now() - start > timeout) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        if (error_message.empty())
                            error_message = "Consumer timeout: consumed "
                                + std::to_string(events_consumed.load()) + "/"
                                + std::to_string(total_events) + " events";
                        break;
                    }
                    if (!pending_future) pending_future = consumer.pull();
                    auto event_opt = pending_future.wait(100);
                    if (event_opt) {
                        pending_future = {};
                        if (event_opt->id() != diaspora::NoMoreEvents) {
                            events_consumed.fetch_add(1);
                            event_opt->acknowledge();
                        }
                    }
                }
            } catch (const std::exception& ex) {
                std::lock_guard<std::mutex> lock(error_mutex);
                if (error_message.empty())
                    error_message = std::string("Consumer error: ") + ex.what();
            }
        });

        for (auto& t : producer_threads) {
            t.join();
        }
        consumer_thread.join();

        {
            std::lock_guard<std::mutex> lock(error_mutex);
            if (!error_message.empty()) {
                FAIL(error_message);
            }
        }

        REQUIRE(events_produced == total_events);
        REQUIRE(events_consumed == total_events);
        REQUIRE(producers_done == num_producers);
    }
}

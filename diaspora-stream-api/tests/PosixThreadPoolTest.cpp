/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <diaspora/PosixThreadPool.hpp>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

TEST_CASE("PosixThreadPool basic functionality", "[posix-threadpool]") {

    SECTION("Create thread pool with zero threads") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{0}};
        REQUIRE(pool.threadCount().count == 0);
        REQUIRE(pool.size() == 0);

        // Work should execute immediately in the calling thread
        std::atomic<int> counter{0};
        pool.pushWork([&counter]() { counter++; });
        REQUIRE(counter == 1);
        REQUIRE(pool.size() == 0);
    }

    SECTION("Create thread pool with single thread") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{1}};
        REQUIRE(pool.threadCount().count == 1);
        REQUIRE(pool.size() == 0);
    }

    SECTION("Create thread pool with multiple threads") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{4}};
        REQUIRE(pool.threadCount().count == 4);
        REQUIRE(pool.size() == 0);
    }
}

TEST_CASE("PosixThreadPool work execution", "[posix-threadpool]") {

    SECTION("Execute single work item") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{2}};
        volatile std::atomic<bool> executed{false};

        pool.pushWork([&executed]() {
            executed = true;
        });

        // Wait for work to complete
        auto start = std::chrono::steady_clock::now();
        while (!executed) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(2)) {
                FAIL("Timeout waiting for work to execute");
            }
        }
        REQUIRE(executed == true);
    }

    SECTION("Execute multiple work items") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{4}};
        volatile std::atomic<int> counter{0};
        const int num_tasks = 100;

        for (int i = 0; i < num_tasks; ++i) {
            pool.pushWork([&counter]() {
                counter++;
            });
        }

        // Wait for all work to complete
        auto start = std::chrono::steady_clock::now();
        while (counter < num_tasks) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(5)) {
                FAIL("Timeout waiting for tasks to complete");
            }
        }
        REQUIRE(counter == num_tasks);
    }

    SECTION("Work items can capture and modify shared state safely") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{2}};
        volatile std::atomic<int> sum{0};
        std::vector<int> values = {1, 2, 3, 4, 5};

        for (int val : values) {
            pool.pushWork([&sum, val]() {
                sum += val;
            });
        }

        // Wait for all work to complete
        auto start = std::chrono::steady_clock::now();
        while (sum < 15) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(2)) {
                FAIL("Timeout waiting for work to complete");
            }
        }
        REQUIRE(sum == 15); // 1 + 2 + 3 + 4 + 5
    }
}

TEST_CASE("PosixThreadPool with priorities", "[posix-threadpool]") {

    SECTION("Work with different priorities all execute eventually") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{2}};
        volatile std::atomic<int> low_prio_executed{0};
        volatile std::atomic<int> med_prio_executed{0};
        volatile std::atomic<int> high_prio_executed{0};
        volatile std::atomic<int> max_prio_executed{0};

        const int tasks_per_priority = 10;

        for (int i = 0; i < tasks_per_priority; ++i) {
            pool.pushWork([&low_prio_executed]() { low_prio_executed++; }, 1);
            pool.pushWork([&med_prio_executed]() { med_prio_executed++; }, 100);
            pool.pushWork([&high_prio_executed]() { high_prio_executed++; }, 1000);
            pool.pushWork([&max_prio_executed]() { max_prio_executed++; },
                         std::numeric_limits<uint64_t>::max());
        }

        // Wait for all work to complete
        auto start = std::chrono::steady_clock::now();
        while (low_prio_executed < tasks_per_priority ||
               med_prio_executed < tasks_per_priority ||
               high_prio_executed < tasks_per_priority ||
               max_prio_executed < tasks_per_priority) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(5)) {
                FAIL("Timeout waiting for prioritized tasks to complete");
            }
        }

        REQUIRE(low_prio_executed == tasks_per_priority);
        REQUIRE(med_prio_executed == tasks_per_priority);
        REQUIRE(high_prio_executed == tasks_per_priority);
        REQUIRE(max_prio_executed == tasks_per_priority);
    }
}

TEST_CASE("PosixThreadPool size tracking", "[posix-threadpool]") {

    SECTION("Size reflects queued work") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{1}};

        // Block the thread
        volatile std::atomic<bool> can_proceed{false};
        pool.pushWork([&]() {
            while (!can_proceed) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // Push more work
        for (int i = 0; i < 5; ++i) {
            pool.pushWork([]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            });
        }

        // Check size
        REQUIRE(pool.size() == 5);

        // Release and wait
        can_proceed = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        REQUIRE(pool.size() == 0);
    }
}

TEST_CASE("PosixThreadPool stress test", "[posix-threadpool]") {

    SECTION("Handle many concurrent tasks") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{8}};
        volatile std::atomic<int> counter{0};
        const int num_tasks = 10000;

        for (int i = 0; i < num_tasks; ++i) {
            pool.pushWork([&counter]() {
                counter.fetch_add(1);
            });
        }

        // Wait for all work to complete
        auto start = std::chrono::steady_clock::now();
        while (counter < num_tasks) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(10)) {
                FAIL("Timeout waiting for tasks to complete");
            }
        }

        REQUIRE(counter == num_tasks);
    }
}

TEST_CASE("PosixThreadPool destruction", "[posix-threadpool]") {

    SECTION("Destructor with no pending work") {
        volatile std::atomic<int> completed{0};
        {
            auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{2}};

            for (int i = 0; i < 10; ++i) {
                pool.pushWork([&completed]() {
                    completed++;
                });
            }

            // Wait for all work to complete before destruction
            auto start = std::chrono::steady_clock::now();
            while (completed < 10) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                auto elapsed = std::chrono::steady_clock::now() - start;
                if (elapsed > std::chrono::seconds(2)) {
                    FAIL("Timeout waiting for work to complete");
                }
            }
        }

        // All work completed before destruction
        REQUIRE(completed == 10);
    }
}

TEST_CASE("PosixThreadPool concurrent access", "[posix-threadpool]") {

    SECTION("Multiple threads can push work concurrently") {
        auto pool = diaspora::PosixThreadPool{diaspora::ThreadCount{4}};
        volatile std::atomic<int> counter{0};
        const int num_pushers = 8;
        const int tasks_per_pusher = 100;

        std::vector<std::thread> pushers;
        for (int i = 0; i < num_pushers; ++i) {
            pushers.emplace_back([&]() {
                for (int j = 0; j < tasks_per_pusher; ++j) {
                    pool.pushWork([&counter]() {
                        counter++;
                    });
                }
            });
        }

        for (auto& t : pushers) {
            t.join();
        }

        // Wait for all work to complete
        auto start = std::chrono::steady_clock::now();
        while (counter < num_pushers * tasks_per_pusher) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(5)) {
                FAIL("Timeout waiting for tasks to complete");
            }
        }
        REQUIRE(counter == num_pushers * tasks_per_pusher);
    }
}

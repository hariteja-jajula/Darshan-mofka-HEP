/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>

// Only compile these tests if Argobots/Thallium is available
#ifdef ENABLE_ARGOBOTS_TESTS

#include <diaspora/ArgobotsThreadPool.hpp>
#include <thallium.hpp>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace tl = thallium;

// Helper class to initialize Argobots runtime
struct ArgobotsScope {
    ArgobotsScope() {
        ABT_init(0, nullptr);
    }
    ~ArgobotsScope() {
        ABT_finalize();
    }
};

TEST_CASE("ArgobotsThreadPool basic functionality", "[argobots-threadpool]") {
    ArgobotsScope abt_scope;

    SECTION("Create thread pool with zero threads (uses caller's pool)") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{0}};
        REQUIRE(pool.threadCount().count == 0);
    }

    SECTION("Create thread pool with single thread") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{1}};
        REQUIRE(pool.threadCount().count == 1);
    }

    SECTION("Create thread pool with multiple threads") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{4}};
        REQUIRE(pool.threadCount().count == 4);
    }

    SECTION("Create from existing thallium pool") {
        auto es = tl::xstream::self();
        auto pool_handle = es.get_main_pools(1)[0];
        auto pool = diaspora::ArgobotsThreadPool{pool_handle};
        REQUIRE(pool.threadCount().count == 0); // Externally managed
    }
}

TEST_CASE("ArgobotsThreadPool work execution", "[argobots-threadpool]") {
    ArgobotsScope abt_scope;

    SECTION("Execute single work item") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{2}};
        volatile std::atomic<bool> executed{false};

        pool.pushWork([&executed]() {
            executed = true;
        });

        // Wait for work to complete
        auto start = std::chrono::steady_clock::now();
        while (!executed) {
            ABT_thread_yield();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(2)) {
                FAIL("Timeout waiting for work to execute");
            }
        }
        REQUIRE(executed == true);
    }

    SECTION("Execute multiple work items") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{4}};
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
            ABT_thread_yield();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(5)) {
                FAIL("Timeout waiting for tasks to complete");
            }
        }
        REQUIRE(counter == num_tasks);
    }

    SECTION("Work items can capture and modify shared state") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{2}};
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
            ABT_thread_yield();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(2)) {
                FAIL("Timeout waiting for work to complete");
            }
        }
        REQUIRE(sum == 15); // 1 + 2 + 3 + 4 + 5
    }
}

TEST_CASE("ArgobotsThreadPool with priorities", "[argobots-threadpool]") {
    ArgobotsScope abt_scope;

    SECTION("Work with different priorities all execute eventually") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{2}};
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
            ABT_thread_yield();
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

TEST_CASE("ArgobotsThreadPool size tracking", "[argobots-threadpool]") {
    ArgobotsScope abt_scope;

    SECTION("Size reflects queued work") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{1}};

        // Block the thread
        thallium::eventual<void> can_proceed;
        pool.pushWork([&]() {
            while (!can_proceed.test()) {
                ABT_thread_yield();
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // Push more work
        for (int i = 0; i < 5; ++i) {
            pool.pushWork([]() {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                ABT_thread_yield();
            });
        }

        // Check size (may include the running task)
        REQUIRE(pool.size() >= 5);

        // Release and wait
        can_proceed.set_value();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Size should decrease significantly
        REQUIRE(pool.size() <= 1);
    }
}

TEST_CASE("ArgobotsThreadPool stress test", "[argobots-threadpool]") {
    ArgobotsScope abt_scope;

    SECTION("Handle many concurrent tasks") {
        auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{8}};
        volatile std::atomic<int> counter{0};
        const int num_tasks = 1000; // Reduced from 10000 for ULT overhead

        for (int i = 0; i < num_tasks; ++i) {
            pool.pushWork([&counter]() {
                counter.fetch_add(1);
            });
        }

        // Wait for all work to complete
        auto start = std::chrono::steady_clock::now();
        while (counter < num_tasks) {
            ABT_thread_yield();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(10)) {
                FAIL("Timeout waiting for tasks to complete");
            }
        }

        REQUIRE(counter == num_tasks);
    }
}

TEST_CASE("ArgobotsThreadPool destruction", "[argobots-threadpool]") {
    ArgobotsScope abt_scope;

    SECTION("Destructor with no pending work") {
        volatile std::atomic<int> completed{0};
        {
            auto pool = diaspora::ArgobotsThreadPool{diaspora::ThreadCount{2}};

            for (int i = 0; i < 10; ++i) {
                pool.pushWork([&completed]() {
                    completed++;
                });
            }

            // Wait for all work to complete before destruction
            auto start = std::chrono::steady_clock::now();
            while (completed < 10) {
                ABT_thread_yield();
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

TEST_CASE("ArgobotsThreadPool with external pool", "[argobots-threadpool]") {
    ArgobotsScope abt_scope;

    SECTION("Can use external thallium pool") {
        auto es = tl::xstream::self();
        auto pool_handle = es.get_main_pools(1)[0];
        auto pool = diaspora::ArgobotsThreadPool{pool_handle};

        volatile std::atomic<bool> executed{false};
        pool.pushWork([&executed]() {
            executed = true;
        });

        auto start = std::chrono::steady_clock::now();
        while (!executed) {
            ABT_thread_yield();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > std::chrono::seconds(2)) {
                FAIL("Timeout waiting for work to execute");
            }
        }
        REQUIRE(executed == true);
    }
}

#endif // ENABLE_ARGOBOTS_TESTS

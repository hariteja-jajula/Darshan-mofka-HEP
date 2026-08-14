/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_POSIX_THREAD_POOL_HPP
#define DIASPORA_POSIX_THREAD_POOL_HPP

#include <diaspora/ThreadPool.hpp>

#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace diaspora {

class PosixThreadPool final : public ThreadPoolInterface {

    struct Work {

        uint64_t              priority;
        std::function<void()> func;

        Work(uint64_t p, std::function<void()> f)
        : priority(p)
        , func{std::move(f)} {}

        friend bool operator<(const Work& lhs, const Work& rhs) {
            return lhs.priority < rhs.priority;
        }
    };

    std::vector<std::thread>                   m_threads;
    std::priority_queue<std::shared_ptr<Work>> m_queue;
    std::queue<std::shared_ptr<Work>>          m_top; // items with max priority
    mutable std::mutex                         m_mtx;
    std::condition_variable                    m_cv;
    bool                                       m_must_stop = false;

    public:

    PosixThreadPool(diaspora::ThreadCount count) {
        m_threads.reserve(count.count);
        for(size_t i = 0; i < count.count; ++i) {
            m_threads.emplace_back([this]() {
                std::unique_lock<std::mutex> lock{m_mtx};
                while(!m_must_stop) {
                    if(m_queue.empty() && m_top.empty()) m_cv.wait(lock);
                    if(!m_top.empty()) {
                        auto work = m_top.front();
                        m_top.pop();
                        lock.unlock();
                        (*work).func();
                        lock.lock();
                    } else if(!m_queue.empty()) {
                        auto work = m_queue.top();
                        m_queue.pop();
                        lock.unlock();
                        (*work).func();
                        lock.lock();
                    }
                }
            });
        }
    }

    ~PosixThreadPool() {
        {
            std::unique_lock<std::mutex> lock{m_mtx};
            m_must_stop = true;
        }
        m_cv.notify_all();
        for(auto& th : m_threads) th.join();
    }

    diaspora::ThreadCount threadCount() const override {
        return diaspora::ThreadCount{m_threads.size()};
    }

    void pushWork(std::function<void()> func,
                  uint64_t priority = std::numeric_limits<uint64_t>::max()) override {
        if(m_threads.size() == 0) {
            func();
        } else {
            {
                std::unique_lock<std::mutex> lock{m_mtx};
                if(priority == std::numeric_limits<uint64_t>::max()) {
                    m_top.push(std::make_shared<Work>(priority, std::move(func)));
                } else {
                    m_queue.push(std::make_shared<Work>(priority, std::move(func)));
                }
            }
            m_cv.notify_one();
        }
    }

    size_t size() const override {
        std::unique_lock<std::mutex> lock{m_mtx};
        return m_queue.size() + m_top.size();
    }
};

}

#endif

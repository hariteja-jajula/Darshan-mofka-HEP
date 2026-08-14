/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_THREAD_POOL_HPP
#define DIASPORA_API_THREAD_POOL_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Future.hpp>

#include <memory>
#include <functional>
#include <limits>

namespace diaspora {

/**
 * @brief Strongly typped size_t meant to represent the number of
 * threads in a ThreadPool.
 */
struct ThreadCount {

    std::size_t count;

    explicit constexpr ThreadCount(std::size_t val)
    : count(val) {}
};

/**
 * @brief A ThreadPool is an object that manages a number of threads
 * and pushes works for them to execute.
 */
class ThreadPoolInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ThreadPoolInterface() = default; // LCOV_EXCL_LINE

    /**
     * @brief Returns the number of underlying threads.
     */
    virtual ThreadCount threadCount() const = 0;

    /**
     * @brief Push work into the thread pool.
     *
     * @param func Function to push.
     * @param priority Priority. Higher priority work should be done first.
     */
    virtual void pushWork(std::function<void()> func,
                          uint64_t priority = std::numeric_limits<uint64_t>::max()) = 0;

    /**
     * @brief Get the number of tasks in the pool, including blocked and running tasks.
     */
    virtual size_t size() const = 0;
};

/**
 * @brief A ThreadPool is an object encapsulating an implementation of a ThreadPoolInterface.
 */
class ThreadPool {

    friend struct PythonBindingHelper;
    friend class Consumer;
    friend class Producer;
    friend class TopicHandle;
    friend class Driver;

    public:

    /**
     * @brief Constructor.
     */
    ThreadPool() = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-constructor.
     */
    ThreadPool(const ThreadPool&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    ThreadPool(ThreadPool&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    ThreadPool& operator=(const ThreadPool&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    ThreadPool& operator=(ThreadPool&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    ~ThreadPool() = default; // LCOV_EXCL_LINE

    /**
     * @brief Returns the number of underlying threads.
     */
    inline ThreadCount threadCount() const {
        return self->threadCount();
    }

    /**
     * @brief Push work into the thread pool.
     *
     * @param func Function to push.
     * @param priority Priority.
     */
    inline void pushWork(std::function<void()> func,
                         uint64_t priority = std::numeric_limits<uint64_t>::max()) const {
        self->pushWork(std::move(func), priority);
    }

    /**
     * @brief Get the number of ULTs in the pool, including blocked and running ULTs.
     */
    inline size_t size() const {
        return self->size();
    }

    /**
     * @brief Checks if the ThreadPool instance is valid.
     */
    inline explicit operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Convert into the underlying std::shared_ptr<ThreadPoolInterface>.
     */
    explicit inline operator std::shared_ptr<ThreadPoolInterface>() const {
        return self;
    }

    /**
     * @brief Try to convert into a reference to the underlying type.
     */
    template<typename T>
    T& as() {
        auto ptr = std::dynamic_pointer_cast<T>(self);
        if(ptr) return *ptr;
        else throw Exception{"Invalid type convertion requested"};
    }

    private:

    /**
     * @brief Constructor is private. Use a Client object
     * to create a ThreadPool instance.
     *
     * @param impl Pointer to implementation.
     */
    ThreadPool(const std::shared_ptr<ThreadPoolInterface>& impl)
    : self{impl} {}

    std::shared_ptr<ThreadPoolInterface> self;
};

}

#endif

/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_ABT_THREAD_POOL_H
#define DIASPORA_API_ABT_THREAD_POOL_H

#include <diaspora/ThreadPool.hpp>
// Note: diaspora-stream-api doesn't depend on thallium, if the user
// wants to use the below class, they have to include thallium themselves
#include <thallium.hpp>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <chrono>

namespace diaspora {

class ArgobotsThreadPool : public diaspora::ThreadPoolInterface {


    /**
     * Argument passed to a ULT; encapsulates the priority,
     * the function to ultimately invoke, and its arguments.
     */
    struct ArgsWrapper {
        uint64_t priority;
        void (*func)(void*);
        void* args;
    };

    /**
     * ULT structure that will be stored and used inside the pool
     * to wrap an ABT_thread and provide an ordering.
     */
    struct ULT {

        ABT_thread m_thread;

        ULT(ABT_thread t)
        : m_thread(t) {}

        bool operator<(const ULT& other) const {
            uint64_t this_prio = 0, other_prio = 0;
            ArgsWrapper *this_args, *other_args;
            ABT_thread_get_arg(m_thread, (void**)&this_args);
            ABT_thread_get_arg(other.m_thread, (void**)&other_args);
            if(this_args) this_prio = this_args->priority;
            if(other_args) other_prio = other_args->priority;
            if(this_prio < other_prio) return true;
            if(this_prio > other_prio) return false;
            return (intptr_t)m_thread < (intptr_t)other.m_thread;
        }
    };

    /**
     * Data attached to the ABT_pool. This pool definition has two
     * structures: an std::priority_queue for ULTs with a priority
     * less than std::numeric_limits<uint64_t>::max(), and a FIFO
     * queue for the ULTs with the maximum priority. Maximum priority
     * is not considered an actual maximum, but a second queue that
     * will be polled half of the time. This is typically because
     * for instance the network progress loop might be in this queue.
     */
    struct PrioPoolData {
        std::mutex               mutex;
        std::condition_variable  cond;
        uint64_t                 cs_count = 0;
        std::priority_queue<ULT> prio_queue;
        std::queue<ULT>          fifo_queue;
    };

    /**
     * @brief Wrapper to access the underlying container.
     */
    struct PriorityQueueAdaptor : public std::priority_queue<ULT> {
        const auto& container() const {
            return this->c;
        }
    };

    /**
     * @brief Wrapper to access the underlying container.
     */
    struct FifoQueueAdaptor : public std::queue<ULT> {
        const auto& container() const {
            return this->c;
        }
    };

    static inline int pool_init(ABT_pool pool, ABT_pool_config config) {
        (void)config;
        auto p_data = new PrioPoolData{};
        return ABT_pool_set_data(pool, p_data);
    }

    static inline void pool_free(ABT_pool pool) {
        PrioPoolData* p_data = nullptr;
        ABT_pool_get_data(pool, (void**)&p_data);
        delete p_data;
    }

    static inline ABT_bool pool_is_empty(ABT_pool pool) {
        PrioPoolData* p_data = nullptr;
        ABT_pool_get_data(pool, (void**)&p_data);
        auto guard = std::unique_lock<std::mutex>{p_data->mutex};
        return p_data->prio_queue.empty() && p_data->fifo_queue.empty();
    }

    static inline size_t pool_get_size(ABT_pool pool) {
        PrioPoolData* p_data = nullptr;
        ABT_pool_get_data(pool, (void**)&p_data);
        auto guard = std::unique_lock<std::mutex>{p_data->mutex};
        return p_data->prio_queue.size() + p_data->fifo_queue.size();
    }

    static inline void pool_push(ABT_pool pool, ABT_unit unit, ABT_pool_context context) {
        (void)context;
        PrioPoolData* p_data = nullptr;
        ABT_pool_get_data(pool, (void**)&p_data);
        {
            ArgsWrapper *wrapper;
            ABT_thread_get_arg((ABT_thread)unit, (void**)&wrapper);

            auto guard = std::unique_lock<std::mutex>{p_data->mutex};
            if(wrapper->priority < std::numeric_limits<uint64_t>::max())
                p_data->prio_queue.emplace((ABT_thread)unit);
            else
                p_data->fifo_queue.emplace((ABT_thread)unit);
        }
        p_data->cond.notify_one();
    }

    static inline void pool_push_many(ABT_pool pool, const ABT_unit *units,
                               size_t num_units, ABT_pool_context context) {
        (void)context;
        PrioPoolData* p_data = nullptr;
        ABT_pool_get_data(pool, (void**)&p_data);
        {
            auto guard = std::unique_lock<std::mutex>{p_data->mutex};
            for(size_t i = 0; i < num_units; ++i) {
                ArgsWrapper *wrapper;
                ABT_thread_get_arg((ABT_thread)units[i], (void**)&wrapper);
                if(wrapper->priority < std::numeric_limits<uint64_t>::max())
                    p_data->prio_queue.emplace((ABT_thread)units[i]);
                else
                    p_data->fifo_queue.emplace((ABT_thread)units[i]);
            }
        }
        if(num_units == 1)
            p_data->cond.notify_one();
        else
            p_data->cond.notify_all();
    }

    static inline ABT_thread pool_pop_wait(ABT_pool pool, double time_secs,
                                           ABT_pool_context context) {
        (void)context;
        PrioPoolData* p_data = nullptr;
        ABT_thread result = ABT_THREAD_NULL;
        ABT_pool_get_data(pool, (void**)&p_data);
        {
            auto guard = std::unique_lock<std::mutex>{p_data->mutex};
            if(p_data->prio_queue.empty() && p_data->fifo_queue.empty())
                p_data->cond.wait_for(guard, std::chrono::duration<double, std::milli>(time_secs*1000));
            p_data->cs_count++;
            if(p_data->cs_count % 2 == 0) {
                if(!p_data->prio_queue.empty()) {
                    result = p_data->prio_queue.top().m_thread;
                    p_data->prio_queue.pop();
                } else if(!p_data->fifo_queue.empty()) {
                    result = p_data->fifo_queue.front().m_thread;
                    p_data->fifo_queue.pop();
                }
            } else {
                if(!p_data->fifo_queue.empty()) {
                    result = p_data->fifo_queue.front().m_thread;
                    p_data->fifo_queue.pop();
                } else if(!p_data->prio_queue.empty()) {
                    result = p_data->prio_queue.top().m_thread;
                    p_data->prio_queue.pop();
                }
            }
        }
        return result;
    }

    static inline ABT_thread pool_pop(ABT_pool pool, ABT_pool_context context) {
        (void)context;
        PrioPoolData* p_data = nullptr;
        ABT_thread result = ABT_THREAD_NULL;
        ABT_pool_get_data(pool, (void**)&p_data);
        {
            auto guard = std::unique_lock<std::mutex>{p_data->mutex};
            p_data->cs_count++;
            if(p_data->cs_count % 2 == 0) {
                if(!p_data->prio_queue.empty()) {
                    result = p_data->prio_queue.top().m_thread;
                    p_data->prio_queue.pop();
                } else if(!p_data->fifo_queue.empty()) {
                    result = p_data->fifo_queue.front().m_thread;
                    p_data->fifo_queue.pop();
                }
            } else {
                if(!p_data->fifo_queue.empty()) {
                    result = p_data->fifo_queue.front().m_thread;
                    p_data->fifo_queue.pop();
                } else if(!p_data->prio_queue.empty()) {
                    result = p_data->prio_queue.top().m_thread;
                    p_data->prio_queue.pop();
                }
            }
        }
        return result;
    }

    static inline void pool_pop_many(ABT_pool pool, ABT_thread *threads,
                                     size_t max_threads, size_t *num_popped,
                                     ABT_pool_context context) {
        (void)context;
        PrioPoolData* p_data = nullptr;
        ABT_pool_get_data(pool, (void**)&p_data);
        {
            auto guard = std::unique_lock<std::mutex>{p_data->mutex};
            *num_popped = 0;
            for(size_t i = 0; i < max_threads; ++i) {
                p_data->cs_count++;
                if(p_data->cs_count % 2 == 0) {
                    if(!p_data->prio_queue.empty()) {
                        threads[i] = p_data->prio_queue.top().m_thread;
                        p_data->prio_queue.pop();
                        *num_popped += 1;
                    } else if(!p_data->fifo_queue.empty()) {
                        threads[i] = p_data->fifo_queue.front().m_thread;
                        p_data->fifo_queue.pop();
                        *num_popped += 1;
                    } else {
                        break;
                    }
                } else {
                    if(!p_data->fifo_queue.empty()) {
                        threads[i] = p_data->fifo_queue.front().m_thread;
                        p_data->fifo_queue.pop();
                        *num_popped += 1;
                    } else if(!p_data->prio_queue.empty()) {
                        threads[i] = p_data->prio_queue.top().m_thread;
                        p_data->prio_queue.pop();
                        *num_popped += 1;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    static inline void pool_print_all(ABT_pool pool, void *arg,
                               void (*print_fn)(void *, ABT_thread)) {
        PrioPoolData* p_data = nullptr;
        ABT_pool_get_data(pool, (void**)&p_data);
        {
            auto guard = std::unique_lock<std::mutex>{p_data->mutex};
            for(auto& ult : static_cast<PriorityQueueAdaptor*>(&p_data->prio_queue)->container()) {
                print_fn(arg, ult.m_thread);
            }
            for(auto& ult : static_cast<FifoQueueAdaptor*>(&p_data->fifo_queue)->container()) {
                print_fn(arg, ult.m_thread);
            }
        }
    }

    static inline ABT_unit pool_create_unit(ABT_pool pool, ABT_thread thread) {
        (void)pool;
        return (ABT_unit)thread;
    }

    static inline void pool_free_unit(ABT_pool pool, ABT_unit unit) {
        (void)pool;
        (void)unit;
    }

    static inline int pool_prio_wait_def_create(ABT_pool_user_def* def) {
        int ret =ABT_pool_user_def_create(
                pool_create_unit,
                pool_free_unit,
                pool_is_empty,
                pool_pop,
                pool_push,
                def);
        if(ret != ABT_SUCCESS)
            return ret;
        ABT_pool_user_def_set_init(*def, pool_init);
        ABT_pool_user_def_set_free(*def, pool_free);
        ABT_pool_user_def_set_get_size(*def, pool_get_size);
        ABT_pool_user_def_set_pop_wait(*def, pool_pop_wait);
        ABT_pool_user_def_set_pop_many(*def, pool_pop_many);
        ABT_pool_user_def_set_push_many(*def, pool_push_many);
        ABT_pool_user_def_set_print_all(*def, pool_print_all);
        return ABT_SUCCESS;
    }

    static inline int pool_prio_wait_def_free(ABT_pool_user_def* def) {
        return ABT_pool_user_def_free(def);
    }

    static inline int thread_create_priority(
            ABT_pool pool, void (*thread_func)(void *), void *arg,
            ABT_thread_attr attr, uint64_t priority, ABT_thread *newthread) {
        auto wrapped_args = new ArgsWrapper{priority, thread_func, arg};
        static auto wrapper_fn = [](void* args) {
            auto wrapped_args = static_cast<ArgsWrapper*>(args);
            (wrapped_args->func)(wrapped_args->args);
            delete wrapped_args;
        };
        int ret = ABT_thread_create(pool, wrapper_fn, wrapped_args, attr, newthread);
        if(ret != ABT_SUCCESS)
            delete wrapped_args;
        return ret;
    }

    public:

    ArgobotsThreadPool(diaspora::ThreadCount tc) {
        if(tc.count > 0) {
            pool_prio_wait_def_create(&m_pool_def);
            ABT_pool_config pool_config = ABT_POOL_CONFIG_NULL;
            ABT_pool pool = ABT_POOL_NULL;
            ABT_pool_create(m_pool_def, pool_config, &pool);
            m_pool = thallium::pool{pool};
            for(std::size_t i=0; i < tc.count; ++i) {
                auto sched = thallium::scheduler::predef::basic_wait;
                m_managed_xstreams.push_back(thallium::xstream::create(sched, m_pool));
            }
        }
    }

    ArgobotsThreadPool(thallium::pool pool)
    : m_pool{pool} {}

    ~ArgobotsThreadPool() {
        for(auto& x : m_managed_xstreams)
            x->join();
        m_managed_xstreams.clear();
        if(m_pool_def) {
            ABT_pool_user_def_free(&m_pool_def);
            ABT_pool pool = m_pool.native_handle();
            ABT_pool_free(&pool);
        }
    }

    void pushWork(std::function<void()> func,
                  uint64_t priority = std::numeric_limits<uint64_t>::max()) override {
        if(threadCount().count == 0) {
            if(m_pool.native_handle() != ABT_POOL_NULL) {
                m_pool.make_thread(std::move(func), thallium::anonymous{});
            } else {
                try {
                    thallium::xstream::self().get_main_pools(1)[0].make_thread(
                        std::move(func), thallium::anonymous{});
                } catch(const thallium::exception&) {
                    func();
                }
            }
            return;
        }
        if(!m_pool_def) { // not custom priority pool, ignore priority
            m_pool.make_thread(std::move(func), thallium::anonymous{});
            thallium::thread::yield();
        } else { // custom priority pool, first argument should be a priority
            struct Args {
                uint64_t              priority;
                std::function<void()> func;
            };
            auto func_wrapper = [](void* args) {
                auto a = static_cast<Args*>(args);
                a->func();
                delete a;
            };
            auto args = new Args{priority, std::move(func)};
            ABT_thread_create(m_pool.native_handle(),
                              func_wrapper, args,
                              ABT_THREAD_ATTR_NULL, nullptr);
        }
    }

    diaspora::ThreadCount threadCount() const override {
        return diaspora::ThreadCount{m_managed_xstreams.size()};
    }

    std::size_t size() const override {
        return m_pool.total_size();
    }

    private:

    thallium::pool                                    m_pool;
    std::vector<thallium::managed<thallium::xstream>> m_managed_xstreams;
    ABT_pool_user_def                                 m_pool_def = nullptr;
};

}

#endif

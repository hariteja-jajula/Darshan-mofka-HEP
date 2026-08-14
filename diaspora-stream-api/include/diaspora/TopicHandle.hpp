/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_TOPIC_HANDLE_HPP
#define DIASPORA_API_TOPIC_HANDLE_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/ArgsUtil.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/DataAllocator.hpp>
#include <diaspora/DataSelector.hpp>
#include <diaspora/Ordering.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/Consumer.hpp>
#include <diaspora/Validator.hpp>
#include <diaspora/Serializer.hpp>

#include <memory>
#include <unordered_set>

namespace diaspora {

class TopicHandleInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~TopicHandleInterface() = default; // LCOV_EXCL_LINE

    /**
     * @brief Returns the name of the topic.
     */
    virtual const std::string& name() const = 0;

    /**
     * @brief Driver that instantiated this TopicHandle.
     */
    virtual std::shared_ptr<DriverInterface> driver() const = 0;

    /**
     * @brief Returns the list of PartitionInfo of the underlying topic.
     */
    virtual const std::vector<PartitionInfo>& partitions() const = 0;

    /**
     * @brief Return the Validator of the topic.
     */
    virtual Validator validator() const = 0;

    /**
     * @brief Return the PartitionSelector of the topic.
     */
    virtual PartitionSelector selector() const = 0;

    /**
     * @brief Return the Serializer of the topic.
     */
    virtual Serializer serializer() const = 0;

    /**
     * @brief Create a Producer object from the full
     * list of optional arguments.
     *
     * @param name Name of the Producer.
     * @param batch_size Batch size.
     * @param max_batch Maximum number of batches (must be >= 1) that can be pending at the same time.
     * @param ordering Whether to enforce strict ordering.
     * @param thread_pool Thread pool.
     * @param options Extra options.
     *
     * @return ProducerInterface instance.
     */
    virtual std::shared_ptr<ProducerInterface>
        makeProducer(std::string_view name,
                     BatchSize batch_size,
                     MaxNumBatches max_batch,
                     Ordering ordering,
                     std::shared_ptr<ThreadPoolInterface> thread_pool,
                     Metadata options) = 0;

    /**
     * @brief Create a Consumer object from the full
     * list of optional and mandatory arguments.
     *
     * @param name Name of the Consumer.
     * @param batch_size Batch size.
     * @param max_batch Maximum number of batches (must be >= 1).
     * @param thread_pool Thread pool.
     * @param data_allocator Data allocator.
     * @param data_selector Data selector.
     * @param targets Indices of the partitions to consumer from.
     * @param options Extra options.
     *
     * @return ConsumerInterface instance.
     */
    virtual std::shared_ptr<ConsumerInterface>
        makeConsumer(std::string_view name,
                     BatchSize batch_size,
                     MaxNumBatches max_batch,
                     std::shared_ptr<ThreadPoolInterface> thread_pool,
                     DataAllocator data_allocator,
                     DataSelector data_selector,
                     const std::vector<size_t>& targets,
                     Metadata options) = 0;

};

/**
 * @brief A TopicHandle object is a handle for a remote topic
 * on a set of servers. It enables invoking the topic's functionalities.
 */
class TopicHandle {

    friend class Consumer;
    friend class Producer;
    friend class Driver;

    public:

    inline TopicHandle() = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-constructor.
     */
    inline TopicHandle(const TopicHandle&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    inline TopicHandle(TopicHandle&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    inline TopicHandle& operator=(const TopicHandle&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    inline TopicHandle& operator=(TopicHandle&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    inline ~TopicHandle() = default; // LCOV_EXCL_LINE

    /**
     * @brief Returns the name of the topic.
     */
    inline const std::string& name() const {
        return self->name();
    }

    /**
     * @brief Driver that instantiated this TopicHandle.
     */
    Driver driver() const;

    /**
     * @brief Creates a Producer object with the specified options.
     * This function allows providing the options in any order,
     * and ommit non-mandatory options. See TopicHandle::makeProducer
     * for the documentation on all the options.
     *
     * @return a Producer object.
     */
    template<typename ... Options>
    inline Producer producer(Options&&... opts) const {
        return makeProducer(
            GetArgOrDefault(std::string_view{""}, std::forward<Options>(opts)...),
            GetArgOrDefault(BatchSize::Adaptive(), std::forward<Options>(opts)...),
            GetArgOrDefault(MaxNumBatches{2}, std::forward<Options>(opts)...),
            GetArgOrDefault(ThreadPool{}, std::forward<Options>(opts)...),
            GetArgOrDefault(Ordering::Strict, std::forward<Options>(opts)...),
            GetArgOrDefaultExactType(Metadata{}, std::forward<Options>(opts)...));
    }

    /**
     * @brief Creates a Consumer object with the specified options.
     *
     * This function allows providing the options in any order,
     * and ommit non-mandatory options. See TopicHandle::makeConsumer
     * for the documentation on all the options.
     *
     * @return a Consumer object.
     */
    template<typename ... Options>
    inline Consumer consumer(std::string_view name, Options&&... opts) const {
        return makeConsumer(name,
            GetArgOrDefault(BatchSize::Adaptive(), std::forward<Options>(opts)...),
            GetArgOrDefault(MaxNumBatches{2}, std::forward<Options>(opts)...),
            GetArgOrDefault(ThreadPool{}, std::forward<Options>(opts)...),
            GetArgOrDefault(DataAllocator{}, std::forward<Options>(opts)...),
            GetArgOrDefault(DataSelector{}, std::forward<Options>(opts)...),
            GetArgOrDefault(std::vector<size_t>(), std::forward<Options>(opts)...),
            GetArgOrDefault(Metadata{}, std::forward<Options>(opts)...));
    }

    /**
     * @brief Returns the list of PartitionInfo of the underlying topic.
     */
    const std::vector<PartitionInfo>& partitions() const {
        return self->partitions();
    }

    /**
     * @brief Return the Validator of the topic.
     */
    inline Validator validator() const {
        return self->validator();
    }

    /**
     * @brief Return the PartitionSelector of the topic.
     */
    inline PartitionSelector selector() const {
        return self->selector();
    }

    /**
     * @brief Return the Serializer of the topic.
     */
    inline Serializer serializer() const {
        return self->serializer();
    }

    /**
     * @brief Checks if the TopicHandle instance is valid.
     */
    inline explicit operator bool() const {
        return static_cast<bool>(self);
    }

    private:

    std::shared_ptr<TopicHandleInterface> self;

    /**
     * @brief Constructor.
     */
    inline TopicHandle(std::shared_ptr<TopicHandleInterface> impl)
    : self{std::move(impl)} {}

    /**
     * @brief Create a Producer object from the full
     * list of optional arguments.
     *
     * @param name Name of the Producer.
     * @param batch_size Batch size.
     * @param max_batch Max number of batches.
     * @param thread_pool Thread pool.
     * @param ordering Whether to enforce strict ordering.
     *
     * @return Producer instance.
     */
    inline Producer makeProducer(std::string_view name,
                          BatchSize batch_size,
                          MaxNumBatches max_batch,
                          ThreadPool thread_pool,
                          Ordering ordering,
                          Metadata options) const {
        return self->makeProducer(
            name, batch_size, max_batch, ordering,
            thread_pool.self, std::move(options));
    }

    /**
     * @brief Create a Consumer object from the full
     * list of optional and mandatory arguments.
     *
     * @param name Name of the Consumer.
     * @param batch_size Batch size.
     * @param max_batch Max number of batches.
     * @param thread_pool Thread pool.
     * @param data_allocator Data allocator.
     * @param data_selector Data selector.
     * @param targets Indices of the partitions to consumer from.
     *
     * @return Consumer instance.
     */
    inline Consumer makeConsumer(std::string_view name,
                          BatchSize batch_size,
                          MaxNumBatches max_batch,
                          ThreadPool thread_pool,
                          DataAllocator data_allocator,
                          DataSelector data_selector,
                          const std::vector<size_t>& targets,
                          Metadata options) const {
        return self->makeConsumer(
            name, batch_size, max_batch,
            thread_pool.self, data_allocator,
            data_selector, targets, std::move(options));
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

};

}

#endif

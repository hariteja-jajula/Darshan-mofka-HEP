/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_PRODUCER_HPP
#define DIASPORA_API_PRODUCER_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/DataView.hpp>
#include <diaspora/EventID.hpp>
#include <diaspora/Future.hpp>
#include <diaspora/ThreadPool.hpp>
#include <diaspora/BatchParams.hpp>
#include <diaspora/Ordering.hpp>

#include <memory>
#include <optional>
#include <variant>

namespace diaspora {

/**
 * Empty structure used in an std::optional returned by Producer(Interface)::flush.
 */
struct Flushed{};

/**
 * @brief Interface for Producer.
 */
class ProducerInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ProducerInterface() = default;

    /**
     * @brief Returns the name of the producer.
     */
    virtual const std::string& name() const = 0;

    /**
     * @brief Returns a copy of the options provided when
     * the Producer was created.
     */
    virtual BatchSize batchSize() const = 0;

    /**
     * @brief Returns the maximum number of batches the
     * Producer is allowed to hold at any time.
     */
    virtual MaxNumBatches maxNumBatches() const = 0;

    /**
     * @brief Returns the ordering consistency of the producer.
     */
    virtual Ordering ordering() const = 0;

    /**
     * @brief Returns the ThreadPool associated with the Producer.
     */
    virtual std::shared_ptr<ThreadPoolInterface> threadPool() const = 0;

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    virtual std::shared_ptr<TopicHandleInterface> topic() const = 0;

    /**
     * @brief Pushes an event into the producer's underlying topic,
     * returning a Future that can be awaited.
     *
     * @param metadata Metadata of the event.
     * @param data Optional data to attach to the event.
     * @param partition Suggested partition number (may be ignored
     *                  by the PartitionSelector).
     *
     * @return a Future<std::optional<EventID>> tracking the asynchronous operation.
     */
    virtual Future<std::optional<EventID>>
        push(Metadata metadata, DataView data,
             std::optional<size_t> partition) = 0;

    /**
     * Flush the producer. This is a non-blocking call returning a future
     * that can be awaited.
     */
    virtual Future<std::optional<Flushed>> flush() = 0;

};

/**
 * @brief A Producer is an object that can emmit events into a its topic.
 */
class Producer {

    friend class TopicHandle;

    public:

    /**
     * @brief Constructor.
     */
    inline Producer() = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-constructor.
     */
    inline Producer(const Producer&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    inline Producer(Producer&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    inline Producer& operator=(const Producer&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    inline Producer& operator=(Producer&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    inline ~Producer() = default; // LCOV_EXCL_LINE

    /**
     * @brief Returns the name of the producer.
     */
    inline const std::string& name() const {
        return self->name();
    }

    /**
     * @brief Returns a copy of the options provided when
     * the Producer was created.
     */
    inline BatchSize batchSize() const {
        return self->batchSize();
    }

    /**
     * @brief Returns the maximum number of batches the
     * Producer is allowed to hold at any time.
     */
    inline MaxNumBatches maxNumBatch() const {
        return self->maxNumBatches();
    }

    /**
     * @brief Returns the ordering consistency of the producer.
     */
    inline Ordering ordering() const {
        return self->ordering();
    }

    /**
     * @brief Returns the ThreadPool associated with the Producer.
     */
    inline ThreadPool threadPool() const {
        return self->threadPool();
    }

    /**
     * @brief Returns the TopicHandle this producer has been created from.
     */
    TopicHandle topic() const;

    /**
     * @brief Checks if the Producer instance is valid.
     */
    explicit inline operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Pushes an event into the producer's underlying topic,
     * returning a Future that can be awaited.
     *
     * @param metadata Metadata of the event.
     * @param data Optional data to attach to the event.
     * @param partition Optional partition.
     *
     * @return a Future<EventID> tracking the asynchronous operation.
     */
    inline Future<std::optional<EventID>>
        push(Metadata metadata, DataView data = DataView{},
             std::optional<size_t> partition = std::nullopt) const {
        return self->push(metadata, data, partition);
    }

    /**
     * Flush the producer. This is a non-blocking call returning a future
     * that can be awaited.
     */
    inline Future<std::optional<Flushed>> flush() {
        return self->flush();
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
     * @brief Constructor.
     */
    inline Producer(const std::shared_ptr<ProducerInterface>& impl)
    : self{impl} {}

    std::shared_ptr<ProducerInterface> self;
};

}

#endif

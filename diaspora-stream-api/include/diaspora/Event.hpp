/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_EVENT_HPP
#define DIASPORA_API_EVENT_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/DataView.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/PartitionSelector.hpp>
#include <diaspora/EventID.hpp>

#include <memory>
#include <vector>

namespace diaspora {

/**
 * @brief The EventInterface is used by streaming drivers
 * to implement their Event.
 */
class EventInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~EventInterface() = default; // LCOV_EXCL_LINE

    /**
     * @brief Get an Event's Metadata.
     */
    virtual const Metadata& metadata() const = 0;

    /**
     * @brief Get an Event's data.
     */
    virtual const DataView& data() const = 0;

    /**
     * @brief Returns information about the partition
     * this Event originates from.
     */
    virtual PartitionInfo partition() const = 0;

    /**
     * @brief Returns the EventID.
     */
    virtual EventID id() const = 0;

    /**
     * @brief Acknowledge the event.
     * Consumers will always restart reading events from the latest
     * acknowledged event in a partition.
     */
    virtual void acknowledge() const = 0;

};

/**
 * @brief An Event object encapsultes Metadata and Event, as well
 * as internal information about the origin of the event, and
 * enables consumers to acknowledge the event.
 */
class Event {

    friend struct PythonBindingHelper;

    public:

    /**
     * @brief Constructor.
     */
    inline Event(std::shared_ptr<EventInterface> impl = nullptr)
    : self{std::move(impl)} {}

    /**
     * @brief Move-constructor.
     */
    inline Event(Event&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-constructor.
     */
    inline Event(const Event&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Copy-assignment operator.
     */
    inline Event& operator=(const Event&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    inline Event& operator=(Event&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    inline ~Event() = default; // LCOV_EXCL_LINE

    /**
     * @brief Get event Event's Metadata.
     */
    inline const Metadata& metadata() const {
        return self->metadata();
    }

    /**
     * @brief Get event Event's Data.
     */
    inline const DataView& data() const {
        return self->data();
    }

    /**
     * @brief Returns information about the partition
     * this Event originates from.
     */
    inline PartitionInfo partition() const {
        return self->partition();
    }

    /**
     * @brief Returns the EventID.
     */
    inline EventID id() const {
        return self->id();
    }

    /**
     * @brief Send a message to the provider to acknowledge the event.
     * Consumers will always restart reading events from the latest
     * acknowledged event in a partition.
     */
    inline void acknowledge() const {
        return self->acknowledge();
    }

    /**
     * @brief Checks if the Event instance is valid.
     */
    explicit inline operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Convert into the underlying std::shared_ptr<EventInterface>.
     */
    explicit inline operator std::shared_ptr<EventInterface>() const {
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

    std::shared_ptr<EventInterface> self;
};

}

#endif

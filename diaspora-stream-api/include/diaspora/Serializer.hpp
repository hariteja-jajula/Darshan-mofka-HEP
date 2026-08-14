/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_SERIALIZER_HPP
#define DIASPORA_API_SERIALIZER_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Archive.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/InvalidMetadata.hpp>
#include <diaspora/Factory.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace diaspora {

/**
 * @brief The SerializerInterface class provides an interface for
 * serializing the Metadata class. Its serialize and deserialize
 * methods should make use of the Archive's write and read methods
 * respectively.
 *
 * A SerializerInterface must also provide functions to convert
 * itself into a Metadata object an back, so that its internal
 * configuration can be stored.
 */
class SerializerInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~SerializerInterface() = default;

    /**
     * @brief Serialize the Metadata into the Archive.
     * Errors whould be handled by throwing a diaspora::Exception.
     *
     * @param archive Archive into which to serialize the metadata.
     * @param metadata Metadata to serialize.
     */
    virtual void serialize(Archive& archive, const Metadata& metadata) const = 0;

    /**
     * @brief Deserialize the Metadata from the Archive.
     * Errors whould be handled by throwing a diaspora::Exception.
     *
     * @param archive Archive from which to deserialize the metadata.
     * @param metadata Metadata to deserialize.
     */
    virtual void deserialize(Archive& archive, Metadata& metadata) const = 0;

    /**
     * @brief Convert the underlying serializer implementation into a Metadata
     * object that can be stored (e.g. if the serializer uses a compression
     * algorithm, the Metadata could contain the parameters used for
     * compression, so that someone could restore a Serializer with the
     * same parameters later when deserializing objects).
     *
     * @param metadata Metadata representing the internals of the serializer.
     */
    virtual Metadata metadata() const = 0;

    /**
     * @note A SerializerInterface class must also provide a static create
     * function with the following prototype, instanciating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::unique_ptr<SerializerInterface> create(const Metadata&);
     */
};

class Serializer {

    friend struct PythonBindingHelper;
    friend class Driver;

    public:

    Serializer(std::shared_ptr<SerializerInterface> impl)
    : self(std::move(impl)) {}

    /**
     * @brief Constructor. Will construct a valid Serializer that simply
     * serializes the Metadata into JSON string.
     */
    Serializer();

    /**
     * @brief Copy-constructor.
     */
    inline Serializer(const Serializer&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    inline Serializer(Serializer&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief copy-assignment operator.
     */
    inline Serializer& operator=(const Serializer&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    inline Serializer& operator=(Serializer&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    inline ~Serializer() = default; // LCOV_EXCL_LINE

    /**
     * @brief Serialize the Metadata into the Archive.
     * Errors whould be handled by throwing a diaspora::Exception.
     *
     * @param archive Archive into which to serialize the metadata.
     * @param metadata Metadata to serialize.
     */
    inline void serialize(Archive& archive, const Metadata& metadata) const {
        self->serialize(archive, metadata);
    }

    /**
     * @brief Deserialize the Metadata from the Archive.
     * Errors whould be handled by throwing a diaspora::Exception.
     *
     * @param archive Archive from which to deserialize the metadata.
     * @param metadata Metadata to deserialize.
     */
    inline void deserialize(Archive& archive, Metadata& metadata) const {
        self->deserialize(archive, metadata);
    }

    /**
     * @brief Convert the underlying serializer implementation into a Metadata
     * object that can be stored (e.g. if the serializer uses a compression
     * algorithm, the Metadata could contain the parameters used for
     * compression, so that someone could restore a Serializer with the
     * same parameters later when deserializing objects).
     *
     * @param metadata Metadata representing the internals of the serializer.
     */
    inline Metadata metadata() const {
        return self->metadata();
    }

    /**
     * @brief Factory function to create a Serializer instance.
     * If the Metadata is not a JSON object or if it doesn't have a "type"
     * field, this function will assume "type" = "default".
     *
     * @param metadata Metadata of the Serializer.
     *
     * @return Serializer instance.
     */
    static Serializer FromMetadata(const Metadata& metadata);

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    explicit inline operator bool() const {
        return static_cast<bool>(self);
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

    std::shared_ptr<SerializerInterface> self;

};

using SerializerFactory = Factory<SerializerInterface, const Metadata&>;

}

#define DIASPORA_REGISTER_SERIALIZER(__prefix__, __name__, __type__) \
    DIASPORA_REGISTER_IMPLEMENTATION_FOR(__prefix__, ::diaspora::SerializerFactory, __type__, __name__)

#endif

/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_VALIDATOR_HPP
#define DIASPORA_API_VALIDATOR_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/DataView.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Factory.hpp>
#include <diaspora/InvalidMetadata.hpp>

#include <functional>
#include <exception>
#include <stdexcept>

namespace diaspora {

/**
 * @brief The ValidatorInterface class provides an interface for
 * validating instances of the Metadata class.
 *
 * A ValidatorInterface must also provide functions to convert
 * itself into a Metadata object and back, so that its internal
 * configuration can be stored.
 */
class ValidatorInterface {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~ValidatorInterface() = default; // LCOV_EXCL_LINE

    /**
     * @brief Validate that the Metadata it correct, throwing an
     * InvalidMetadata exception is the Metadata is not valid.
     * The Data associated with the Metadata is also provided,
     * although most validator are only meant to validate the
     * Metadata, not the Data content.
     *
     * @param metadata Metadata to validate.
     * @param data Associated data.
     */
    virtual void validate(const Metadata& metadata, const DataView& data) const = 0;

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    virtual Metadata metadata() const = 0;


    /**
     * @note A ValidatorInterface class must also provide a static Create
     * function with the following prototype, instantiating a shared_ptr of
     * the class from the provided Metadata:
     *
     * static std::shared_ptr<ValidatorInterface> create(const diaspora::Metadata&);
     */
};

class Validator {

    friend struct PythonBindingHelper;
    friend class Driver;

    public:

    Validator(std::shared_ptr<ValidatorInterface> impl)
    : self{std::move(impl)} {}

    /**
     * @brief Constructor. Will construct a valid Validator that accepts
     * any Metadata correctly formatted in JSON.
     */
    Validator();

    /**
     * @brief Copy-constructor.
     */
    inline Validator(const Validator&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    inline Validator(Validator&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief copy-assignment operator.
     */
    inline Validator& operator=(const Validator&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-assignment operator.
     */
    inline Validator& operator=(Validator&&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Destructor.
     */
    inline ~Validator() = default; // LCOV_EXCL_LINE

    /**
     * @brief Checks for the validity of the underlying pointer.
     */
    inline explicit operator bool() const {
        return static_cast<bool>(self);
    }

    /**
     * @brief Validate that the Metadata it correct, throwing an
     * InvalidMetadata exception is the Metadata is not valid.
     * The Data associated with the Metadata is also provided,
     * although most validator are only meant to validate the
     * Metadata, not the Data content.
     *
     * @param metadata Metadata to validate.
     * @param data Associated data.
     */
    inline void validate(const Metadata& metadata, const DataView& data) const {
        self->validate(metadata, data);
    }

    /**
     * @brief Convert the underlying validator implementation into a Metadata
     * object that can be stored (e.g. if the validator uses a JSON schema
     * the Metadata could contain that schema).
     */
    inline Metadata metadata() const {
        return self->metadata();
    }

    /**
     * @brief Factory function to create a Validator instance.
     * The Metadata is expected to be a JSON object with at least
     * a "type" field. The type can be in the form "name:library.so"
     * if library.so must be loaded to access the validator.
     * If the "type" field is not provided, it is assumed to be
     * "default".
     *
     * @param metadata Metadata of the Validator.
     *
     * @return Validator instance.
     */
    static Validator FromMetadata(const Metadata& metadata);

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

    std::shared_ptr<ValidatorInterface> self;
};

using ValidatorFactory = Factory<ValidatorInterface, const Metadata&>;

#define DIASPORA_REGISTER_VALIDATOR(__prefix__, __name__, __type__) \
    DIASPORA_REGISTER_IMPLEMENTATION_FOR(__prefix__, ::diaspora::ValidatorFactory, __type__, __name__)

}

#endif

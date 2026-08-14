/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_FACTORY_HPP
#define DIASPORA_API_FACTORY_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>
#include <dlfcn.h>
#include <unordered_map>
#include <functional>
#include <memory>
#include <iostream>
#include <vector>

namespace diaspora {

template <typename Base, typename... Args>
class Factory;

template <typename FactoryType, typename Derived>
struct Registrar;

template <typename Base, typename... Args>
class Factory {

    public:

    static std::shared_ptr<Base> create(const std::string& key, Args&&... args) {
        auto& factory = instance();
        std::string name = key;
        std::size_t col = key.find(":");
        if (col != std::string::npos) {
            name = key.substr(0, col);
            const auto path = key.substr(col + 1);
            auto it = factory.m_creator_fn.find(name);
            if (it == factory.m_creator_fn.end()) {
                if(dlopen(path.c_str(), RTLD_NOW) == nullptr) {
                    throw Exception(
                            std::string{"Could not dlopen "} + path + ":" + dlerror());
                }
            }
        }
        auto it = factory.m_creator_fn.find(name);
        if(it == factory.m_creator_fn.end() && col == std::string::npos) {
            // assume the library may be named "lib<name>.so"
            auto libname = std::string{"lib"} + name + ".so";
            if(dlopen(libname.c_str(), RTLD_NOW) == nullptr) {
                throw Exception(
                    std::string{"Trying to load lib"} + name + ".so failed: " + dlerror());
            }
            it = factory.m_creator_fn.find(name);
        }
        if (it == factory.m_creator_fn.end())
            throw Exception(std::string("Factory method not found for type ") +  name);
        return it->second(std::forward<Args>(args)...);
    }

    private:

    template <typename FactoryType, typename Derived>
    friend struct Registrar;

    using CreatorFunction = std::function<std::shared_ptr<Base>(Args...)>;

    static Factory& instance();

    void registerCreator(const std::string& key, CreatorFunction creator) {
        m_creator_fn[key] = std::move(creator);
    }

    std::unordered_map<std::string, CreatorFunction> m_creator_fn;
};

template <typename FactoryType, typename Derived>
struct Registrar {

    explicit Registrar(const std::string& key) {
        auto& factory = FactoryType::instance();
        factory.registerCreator(key, Derived::create);
    }

};

}

#define DIASPORA_INSTANTIATE_FACTORY(__type__, ...)            \
    template class ::diaspora::Factory<__type__, __VA_ARGS__>; \
    template<> ::diaspora::Factory<__type__, __VA_ARGS__>&     \
    ::diaspora::Factory<__type__, __VA_ARGS__>::instance() {   \
        static Factory factory;                                \
        return factory;                                        \
    }

#define DIASPORA_REGISTER_IMPLEMENTATION_FOR(__prefix__, __factory__, __derived__, __name__) \
    static ::diaspora::Registrar<__factory__, __derived__> \
    __diasporaRegistrarFor ## __prefix__ ## _ ## __derived__ ## _ ## __name__{#__name__}

#endif

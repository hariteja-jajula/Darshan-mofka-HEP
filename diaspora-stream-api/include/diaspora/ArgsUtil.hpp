/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_ARGS_UTIL_HPP
#define DIASPORA_API_ARGS_UTIL_HPP

#include <diaspora/ForwardDcl.hpp>

#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>

namespace diaspora {

template<typename Expected>
decltype(auto) GetArgOrDefault(Expected&& exp) {
    return std::forward<Expected>(exp);
}

template<typename Expected, typename T1, typename ... Ts>
decltype(auto) GetArgOrDefault(Expected&& exp, T1&& arg1, Ts&&... args) {
    if constexpr (std::is_same_v<std::decay_t<Expected>, std::decay_t<T1>>) {
        return std::forward<T1>(arg1);
    } else {
        if constexpr (std::is_constructible_v<Expected, T1>) {
            return Expected(arg1);
        } else {
            return GetArgOrDefault(exp, std::forward<Ts>(args)...);
        }
    }
}

template<typename Expected>
decltype(auto) GetArgOrDefaultExactType(Expected&& exp) {
    return std::forward<Expected>(exp);
}

template<typename Expected, typename T1, typename ... Ts>
decltype(auto) GetArgOrDefaultExactType(Expected&& exp, T1&& arg1, Ts&&... args) {
    if constexpr (std::is_same_v<std::decay_t<Expected>, std::decay_t<T1>>) {
        return std::forward<T1>(arg1);
    } else {
        return GetArgOrDefault(exp, std::forward<Ts>(args)...);
    }
}

template<typename Expected>
decltype(auto) GetArg() {
    static_assert(std::is_same_v<void,Expected>, "Could not find mandatory argument of Expected type");
    return Expected{};
}

template<typename Expected, typename T1, typename ... Ts>
decltype(auto) GetArg(T1&& arg1, Ts&&... args) {
    if constexpr (std::is_same_v<std::decay_t<Expected>, std::decay_t<T1>>) {
        return std::forward<T1>(arg1);
    } else {
        if constexpr (std::is_constructible_v<Expected, T1>) {
            return Expected(arg1);
        } else {
            return GetArg<Expected>(std::forward<Ts>(args)...);
        }
    }
}

}

#endif

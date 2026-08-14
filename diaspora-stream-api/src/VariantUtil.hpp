/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_VARIANT_UTIL_HPP
#define DIASPORA_VARIANT_UTIL_HPP

namespace diaspora {

template<class... Ts>
struct Overloaded : Ts... { using Ts::operator()...; };

template<class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

}

#endif

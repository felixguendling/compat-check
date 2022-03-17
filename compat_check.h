#pragma once

#include "bytes/iobuf_istreambuf.h"
#include "json/json.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "storage/index_state.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

#include <rapidjson/istreamwrapper.h>

#include <string_view>

inline void verify(bool const b) {
    if (!b) {
        throw std::runtime_error{"verify failed"};
    }
}

void read_value(rapidjson::Value const& v, int64_t& target) {
    verify(v.IsInt64());
    target = v.GetInt64();
}

void read_value(rapidjson::Value const& v, uint64_t& target) {
    verify(v.IsUint64());
    target = v.GetUint64();
}

void read_value(rapidjson::Value const& v, uint32_t& target) {
    verify(v.IsUint());
    target = v.GetUint();
}

void read_value(rapidjson::Value const& v, int32_t& target) {
    verify(v.IsInt());
    target = v.GetInt();
}

template<typename T, typename Tag, typename IsConstexpr>
void read_value(
  rapidjson::Value const& v,
  detail::base_named_type<T, Tag, IsConstexpr>& target) {
    auto t = T{};
    read_value(v, t);
    target = detail::base_named_type<T, Tag, IsConstexpr>{t};
}

template<typename T, size_t A>
void read_value(rapidjson::Value const& v, fragmented_vector<T, A>& target) {
    for (auto const& v : v.GetArray()) {
        auto t = T{};
        read_value(v, t);
        target.push_back(t);
    }
}

template<typename T>
void read_member(rapidjson::Value const& v, char const* key, T& target) {
    if (auto const it = v.FindMember(key); it != v.MemberEnd()) {
        read_value(it->value, target);
    }
}

inline ss::future<void> read_corpus(std::string_view dir) {
    auto const from_json = [](rapidjson::Value& w) {
        storage::index_state st;

        verify(w.IsObject());

        read_member(w, "bitflags", st.bitflags);

        read_member(w, "base_offset", st.base_offset);

        model::timestamp::type base_t;
        read_member(w, "base_timestamp", base_t);
        st.base_timestamp = model::timestamp{base_t};

        model::timestamp::type max_t;
        read_member(w, "max_timestamp", max_t);
        st.max_timestamp = model::timestamp{base_t};

        read_member(w, "relative_offset_index", st.relative_offset_index);

        read_member(w, "relative_time_index", st.relative_time_index);

        read_member(w, "position_index", st.position_index);

        return st;
    };

    iobuf buf; // TODO real file I/O
    iobuf_istreambuf ibuf(buf);
    std::istream stream(&ibuf);
    rapidjson::Document m;
    rapidjson::IStreamWrapper wrapper(stream);
    m.ParseStream(wrapper);
    auto const st = from_json(m);

    verify(st == serde::from_iobuf<storage::index_state>(std::move(buf)));

    return ss::make_ready_future<>();
}

inline ss::future<void> write_corpus(std::string_view corpus_dir) {
    auto const make_random_index_state = []() {
        storage::index_state st;
        st.bitflags = random_generators::get_int<uint32_t>();
        st.base_offset = model::offset(random_generators::get_int<int64_t>());
        st.max_offset = model::offset(random_generators::get_int<int64_t>());
        st.base_timestamp = model::timestamp(
          random_generators::get_int<int64_t>());
        st.max_timestamp = model::timestamp(
          random_generators::get_int<int64_t>());

        const auto n = random_generators::get_int(1, 10000);
        for (auto i = 0; i < n; ++i) {
            st.add_entry(
              random_generators::get_int<uint32_t>(),
              random_generators::get_int<uint32_t>(),
              random_generators::get_int<uint64_t>());
            return st;
        }

        return st;
    };

    auto const to_json = [](
                           rapidjson::Writer<rapidjson::StringBuffer>& w,
                           storage::index_state const& st) {
        w.StartObject();

        w.String("bitflags");
        json::rjson_serialize(w, st.bitflags);

        w.String("base_offset");
        json::rjson_serialize(w, st.base_offset);

        w.String("base_timestamp");
        json::rjson_serialize(w, st.base_timestamp.value());

        w.String("max_timestamp");
        json::rjson_serialize(w, st.max_timestamp.value());

        w.String("relative_offset_index");
        json::rjson_serialize(w, st.relative_offset_index);

        w.String("relative_time_index");
        json::rjson_serialize(w, st.relative_time_index);

        w.String("position_index");
        json::rjson_serialize(w, st.position_index);

        w.EndObject();
    };

    // generate random data
    auto st = make_random_index_state();

    // write random data to corpus_dir/values.json
    auto sb = rapidjson::StringBuffer{};
    auto json_buf = rapidjson::Writer<rapidjson::StringBuffer>{sb};
    to_json(json_buf, st);

    auto json_out_f = ss::file{};
    auto json_out = co_await ss::make_file_output_stream(
      ss::file(json_out_f.dup()));
    co_await json_out.write(sb.GetString(), sb.GetLength());
    co_await json_out.flush();
    co_await json_out.close();

    // write random data to segment index
    auto out_f = ss::file{};
    co_await out_f.truncate(0);
    auto out = co_await ss::make_file_output_stream(ss::file(out_f.dup()));
    auto b = serde::to_iobuf(st.copy());
    for (const auto& f : b) {
        co_await out.write(f.get(), f.size());
    }
    co_await out.flush();
    co_await out.close();
}

#pragma once

// Minimal JSON value type + serializer (zero external dependencies).
// Used by the gateway endpoints instead of manual string concatenation.
// Objects preserve insertion order; numbers print as integers when whole.
//
// Usage:
//   json::obj({{"status", "ok"}, {"nodes", 21}, {"nested", json::obj({...})}})
//   json::array()  -> push_back(json::obj({...})) etc.
// Scalars convert implicitly via the Value(T) constructor.

#include <string>
#include <vector>
#include <utility>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <cstdint>
#include <type_traits>

namespace influmapper {
namespace json {

struct Value {
    enum class Type { Null, Bool, Number, String, Array, Object };

    Type type = Type::Null;
    bool boolean = false;
    double number = 0.0;
    std::string str;
    std::vector<Value> arr;
    std::vector<std::pair<std::string, Value>> obj;

    Value() = default;

    // Implicit construction from scalars so obj({{"k", v}, ...}) accepts
    // strings, numbers and bools directly. Value itself is excluded (copy ctor).
    template <typename T,
              typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Value>>>
    Value(const T& v) : Value(to_value(v)) {}

private:
    // Scalar -> Value conversions. size_t is intentionally absent: it is the
    // same type as uint64_t on LP64/LLP64 targets, which would be a redefinition.
    static Value to_value(bool b)               { Value v; v.type = Type::Bool; v.boolean = b; return v; }
    static Value to_value(int i)                { return num(static_cast<double>(i)); }
    static Value to_value(int64_t i)            { return num(static_cast<double>(i)); }
    static Value to_value(uint32_t i)           { return num(static_cast<double>(i)); }
    static Value to_value(uint64_t i)           { return num(static_cast<double>(i)); }
    static Value to_value(float f)              { return num(f); }
    static Value to_value(double d)             { return num(d); }
    static Value to_value(const char* s)        { Value v; v.type = Type::String; v.str = s; return v; }
    static Value to_value(const std::string& s) { Value v; v.type = Type::String; v.str = s; return v; }
    static Value to_value(const Value& v)       { return v; }

    static Value num(double n) {
        Value v; v.type = Type::Number; v.number = n; return v;
    }

public:
    // ---- serialization ----
    std::string dump() const {
        std::ostringstream ss;
        write_to(ss);
        return ss.str();
    }

private:
    static void escape_into(std::ostringstream& ss, const std::string& s) {
        ss << '"';
        for (unsigned char c : s) {
            switch (c) {
                case '"':  ss << "\\\""; break;
                case '\\': ss << "\\\\"; break;
                case '\b': ss << "\\b";  break;
                case '\f': ss << "\\f";  break;
                case '\n': ss << "\\n";  break;
                case '\r': ss << "\\r";  break;
                case '\t': ss << "\\t";  break;
                default:
                    if (c < 0x20) {
                        ss << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                           << static_cast<int>(c) << std::dec << std::setfill(' ');
                    } else {
                        ss << static_cast<char>(c);
                    }
            }
        }
        ss << '"';
    }

    void write_to(std::ostringstream& ss) const {
        switch (type) {
            case Type::Null:   ss << "null"; break;
            case Type::Bool:   ss << (boolean ? "true" : "false"); break;
            case Type::Number:
                if (std::isfinite(number)) {
                    ss << std::setprecision(15) << number;
                } else {
                    ss << "null";
                }
                break;
            case Type::String: escape_into(ss, str); break;
            case Type::Array:
                ss << '[';
                for (size_t i = 0; i < arr.size(); ++i) {
                    if (i) ss << ',';
                    arr[i].write_to(ss);
                }
                ss << ']';
                break;
            case Type::Object:
                ss << '{';
                for (size_t i = 0; i < obj.size(); ++i) {
                    if (i) ss << ',';
                    escape_into(ss, obj[i].first);
                    ss << ':';
                    obj[i].second.write_to(ss);
                }
                ss << '}';
                break;
        }
    }
};

// ---- Free factory functions (avoid member-name collisions) ----

inline Value null()    { return Value(); }
inline Value array()   { Value v; v.type = Value::Type::Array; return v; }
inline Value boolean_v(bool b) { Value v; v.type = Value::Type::Bool; v.boolean = b; return v; }
inline Value num(double n)     { Value v; v.type = Value::Type::Number; v.number = n; return v; }
inline Value integer(int64_t n){ return num(static_cast<double>(n)); }
inline Value str(std::string s) { Value v; v.type = Value::Type::String; v.str = std::move(s); return v; }
inline Value str(const char* s) { return str(std::string(s)); }

inline Value object(std::vector<std::pair<std::string, Value>> fields) {
    Value v; v.type = Value::Type::Object; v.obj = std::move(fields); return v;
}

// Convenience: object from initializer list of {key, Value} pairs.
// Scalars convert implicitly, e.g. obj({{"a", 1}, {"b", "x"}, {"c", true}})
inline Value obj(std::initializer_list<std::pair<const char*, Value>> fields) {
    Value v; v.type = Value::Type::Object;
    v.obj.reserve(fields.size());
    for (const auto& f : fields) v.obj.emplace_back(f.first, std::move(f.second));
    return v;
}

using Json = Value;

} // namespace json
} // namespace influmapper

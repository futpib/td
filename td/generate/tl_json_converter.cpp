//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2026
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "tl_json_converter.h"

#include "td/tl/tl_simple.h"

#include "td/utils/buffer.h"
#include "td/utils/common.h"
#include "td/utils/filesystem.h"
#include "td/utils/FlatHashSet.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/StringBuilder.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace td {

using Mode = tl::TL_writer::Mode;

static bool is_bytes_like(const tl::simple::Type *type) {
  return type->type == tl::simple::Type::Bytes || type->type == tl::simple::Type::SecureBytes ||
         type->type == tl::simple::Type::Int128 || type->type == tl::simple::Type::Int256 ||
         type->type == tl::simple::Type::Int512;
}

static bool need_bytes(const tl::simple::Type *type) {
  return is_bytes_like(type) ||
         (type->type == tl::simple::Type::Vector && need_bytes(type->vector_value_type));
}

static bool is_suitable(int file_number, int file_count, int &counter) {
  if (file_count <= 1) {
    return true;
  }
  counter++;
  return counter % (file_count - 1) == file_number - 1;
}

template <class T>
void gen_to_json_constructor(StringBuilder &sb, const T *constructor, bool is_header,
                             td::FlatHashSet<std::string> &to_json_types, const std::string &api_name,
                             bool is_function = false) {
  sb << "void to_json(JsonValueScope &jv, "
     << "const " << api_name << "::" << tl::simple::gen_cpp_name(constructor->name) << " &object)";
  if (is_header) {
    if (!is_function) {
      to_json_types.insert(constructor->name);
    }
    sb << ";\n\n";
    return;
  }
  sb << " {\n";
  sb << "  auto jo = jv.enter_object();\n";
  sb << "  jo(\"@type\", \"" << tl::simple::gen_cpp_name(constructor->name) << "\");\n";
  for (auto &arg : constructor->args) {
    auto field_name = tl::simple::gen_cpp_field_name(arg.name);
    bool is_custom = arg.type->type == tl::simple::Type::Custom;

    auto object = PSTRING() << "object." << field_name;
    if (is_custom) {
      sb << "  if (" << object << ") {\n  ";
    }
    if (is_bytes_like(arg.type)) {
      object = PSTRING() << "base64_encode(" << object << ")";
    } else if (arg.type->type == tl::simple::Type::SecureString) {
      object = PSTRING() << "base64_encode(" << object << ")";
    } else if (arg.type->type == tl::simple::Type::Vector && need_bytes(arg.type->vector_value_type)) {
      object = PSTRING() << "JsonVectorBytes<decltype(" << object << ")::value_type>{" << object << "}";
    } else if (arg.type->type == tl::simple::Type::Bool) {
      object = PSTRING() << "JsonBool{" << object << "}";
    } else if (arg.type->type == tl::simple::Type::Int64) {
      object = PSTRING() << "JsonInt64{" << object << "}";
    } else if (arg.type->type == tl::simple::Type::Vector &&
               arg.type->vector_value_type->type == tl::simple::Type::Int64) {
      object = PSTRING() << "JsonVectorInt64{" << object << "}";
    }
    if (is_custom) {
      sb << "  jo(\"" << arg.name << "\", ToJson(*" << object << "));\n";
    } else if (arg.type->type == tl::simple::Type::Int64 || arg.type->type == tl::simple::Type::Vector) {
      sb << "  jo(\"" << arg.name << "\", ToJson(" << object << "));\n";
    } else {
      sb << "  jo(\"" << arg.name << "\", " << object << ");\n";
    }
    if (is_custom) {
      sb << "  }\n";
    }
  }
  sb << "}\n\n";
}

void gen_to_json(StringBuilder &sb, const tl::simple::Schema &schema, bool is_header, Mode mode, int file_number,
                 int file_count, int &counter, td::FlatHashSet<std::string> &to_json_types,
                 const std::string &api_name) {
  for (auto *custom_type : schema.custom_types) {
    if (!((custom_type->is_query_ && mode != Mode::Server) || (custom_type->is_result_ && mode != Mode::Client))) {
      continue;
    }
    if (!is_suitable(file_number, file_count, counter)) {
      continue;
    }
    if (custom_type->constructors.size() > 1) {
      auto type_name = tl::simple::gen_cpp_name(custom_type->name);
      sb << "void to_json(JsonValueScope &jv, const " << api_name << "::" << type_name << " &object)";
      if (is_header) {
        sb << ";\n\n";
      } else {
        sb << " {\n"
           << "  " << api_name << "::downcast_call(const_cast<" << api_name << "::" << type_name
           << " &>(object), [&jv](const auto &object) { "
              "to_json(jv, object); });\n"
           << "}\n\n";
      }
    }
    for (auto *constructor : custom_type->constructors) {
      gen_to_json_constructor(sb, constructor, is_header, to_json_types, api_name);
    }
  }
  if (mode == Mode::Server) {
    return;
  }
  for (auto *function : schema.functions) {
    if (is_suitable(file_number, file_count, counter)) {
      gen_to_json_constructor(sb, function, is_header, to_json_types, api_name, /*is_function=*/true);
    }
  }
}

template <class T>
void gen_from_json_constructor(StringBuilder &sb, const T *constructor, bool is_header,
                               const std::string &api_name) {
  sb << "Status from_json(" << api_name << "::" << tl::simple::gen_cpp_name(constructor->name) << " &to, JsonObject &from)";
  if (is_header) {
    sb << ";\n\n";
  } else {
    sb << " {\n";
    for (auto &arg : constructor->args) {
      sb << "  TRY_STATUS(from_json" << (need_bytes(arg.type) ? "_bytes" : "") << "(to."
         << tl::simple::gen_cpp_field_name(arg.name) << ", from.extract_field(\"" << tl::simple::gen_cpp_name(arg.name)
         << "\")));\n";
    }
    sb << "  return Status::OK();\n";
    sb << "}\n\n";
  }
}

void gen_from_json(StringBuilder &sb, const tl::simple::Schema &schema, bool is_header, Mode mode, int file_number,
                   int file_count, int &counter, const std::string &api_name) {
  for (auto *custom_type : schema.custom_types) {
    if (!((custom_type->is_query_ && mode != Mode::Client) || (custom_type->is_result_ && mode != Mode::Server))) {
      continue;
    }
    for (auto *constructor : custom_type->constructors) {
      if (is_suitable(file_number, file_count, counter)) {
        gen_from_json_constructor(sb, constructor, is_header, api_name);
      }
    }
  }
  if (mode == Mode::Client) {
    return;
  }
  for (auto *function : schema.functions) {
    if (is_suitable(file_number, file_count, counter)) {
      gen_from_json_constructor(sb, function, is_header, api_name);
    }
  }
}

using Vec = std::vector<std::pair<int32, std::string>>;
void gen_tl_constructor_from_string(StringBuilder &sb, Slice name, const Vec &vec, bool is_header,
                                    const std::string &api_name) {
  sb << "Result<int32> tl_constructor_from_string(" << api_name << "::" << name << " *object, const std::string &str)";
  if (is_header) {
    sb << ";\n\n";
    return;
  }
  sb << " {\n";
  sb << "  static const FlatHashMap<Slice, int32, SliceHash> m = {\n";

  bool is_first = true;
  for (auto &p : vec) {
    if (is_first) {
      is_first = false;
    } else {
      sb << ",\n";
    }
    sb << "    {\"" << p.second << "\", " << p.first << "}";
  }
  sb << "\n  };\n";
  sb << "  auto it = m.find(str);\n";
  sb << "  if (it == m.end()) {\n"
     << "    return Status::Error(PSLICE() << \"Unknown class \\\"\" << str << \"\\\"\");\n"
     << "  }\n"
     << "  return it->second;\n";
  sb << "}\n\n";
}

void gen_tl_constructor_from_string(StringBuilder &sb, const tl::simple::Schema &schema, bool is_header, Mode mode,
                                    int file_number, int file_count, int &counter,
                                    const std::string &api_name) {
  Vec vec_for_nullary;
  for (auto *custom_type : schema.custom_types) {
    if (!((custom_type->is_query_ && mode != Mode::Client) || (custom_type->is_result_ && mode != Mode::Server))) {
      continue;
    }
    Vec vec;
    for (auto *constructor : custom_type->constructors) {
      vec.emplace_back(constructor->id, constructor->name);
      vec_for_nullary.push_back(vec.back());
    }

    if (vec.size() > 1) {
      if (is_suitable(file_number, file_count, counter)) {
        gen_tl_constructor_from_string(sb, tl::simple::gen_cpp_name(custom_type->name), vec, is_header, api_name);
      }
    }
  }
  if (file_number != 1 % file_count) {
    return;
  }
  gen_tl_constructor_from_string(sb, "Object", vec_for_nullary, is_header, api_name);

  if (mode == Mode::Client) {
    return;
  }
  Vec vec_for_function;
  for (auto *function : schema.functions) {
    vec_for_function.emplace_back(function->id, function->name);
  }
  gen_tl_constructor_from_string(sb, "Function", vec_for_function, is_header, api_name);
}

void gen_json_converter_file(const tl::simple::Schema &schema, const std::string &file_name_base, bool is_header,
                             Mode mode, int file_number, int file_count, td::FlatHashSet<std::string> &to_json_types,
                             const std::string &api_name) {
  string file_name_suffix;
  if (file_count > 1) {
    file_name_suffix = "_" + td::to_string(file_number);
  }
  auto file_name = is_header ? file_name_base + file_name_suffix + ".h" : file_name_base + file_name_suffix + ".cpp";
  auto old_file_content = [&] {
    auto r_content = read_file(file_name);
    if (r_content.is_error()) {
      return BufferSlice();
    }
    return r_content.move_as_ok();
  }();

  std::string buf(2000000, ' ');
  StringBuilder sb(buf);

  if (is_header) {
    sb << "#pragma once\n\n";

    sb << "#include \"td/telegram/" << api_name << ".h\"\n\n";

    sb << "#include \"td/utils/JsonBuilder.h\"\n";
    sb << "#include \"td/utils/Status.h\"\n\n";
  } else {
    sb << "#include \"" << file_name_base << ".h\"\n\n";

    sb << "#include \"td/telegram/" << api_name << ".h\"\n";
    sb << "#include \"td/telegram/" << api_name << ".hpp\"\n\n";

    sb << "#include \"td/tl/tl_json.h\"\n\n";

    sb << "#include \"td/utils/base64.h\"\n";
    sb << "#include \"td/utils/common.h\"\n";
    sb << "#include \"td/utils/FlatHashMap.h\"\n";
    sb << "#include \"td/utils/Slice.h\"\n\n";
  }
  sb << "namespace td {\n";
  sb << "namespace " << api_name << " {\n";
  if (is_header) {
    sb << "\nvoid to_json(JsonValueScope &jv, const " << api_name << "::object_ptr<Object> &value);\n";
    sb << "\nStatus from_json(" << api_name << "::object_ptr<Function> &to, td::JsonValue from);\n";
    sb << "\nvoid to_json(JsonValueScope &jv, const Object &object);\n\n";
  } else if (file_number == 0) {
    sb << "\nvoid to_json(JsonValueScope &jv, const " << api_name << "::object_ptr<Object> &value) {\n"
       << "  td::to_json(jv, value);\n"
       << "}\n\n";

    sb << "Status from_json(" << api_name << "::object_ptr<Function> &to, td::JsonValue from) {\n"
       << "  return td::from_json(to, std::move(from));\n"
       << "}\n\n";

    sb << "void to_json(JsonValueScope &jv, const Object &object) {\n"
       << "  switch (object.get_id()) {\n";
    std::vector<std::string> type_names;
    for (const auto &type : to_json_types) {
      type_names.push_back(tl::simple::gen_cpp_name(type));
    }
    std::sort(type_names.begin(), type_names.end());
    for (const auto &type_name : type_names) {
      sb << "    case " << api_name << "::" << type_name << "::ID:\n";
      sb << "      return static_cast<void(*)(JsonValueScope &, const " << api_name << "::" << type_name
         << " &)>(" << api_name << "::to_json)(jv, static_cast<const " << api_name << "::" << type_name << " &>(object));\n";
    }
    sb << "    default:\n";
    sb << "      UNREACHABLE();\n";
    sb << "  }\n";
    sb << "}\n\n";
  }
  int counter = 0;
  gen_tl_constructor_from_string(sb, schema, is_header, mode, file_number, file_count, counter, api_name);
  gen_from_json(sb, schema, is_header, mode, file_number, file_count, counter, api_name);
  gen_to_json(sb, schema, is_header, mode, file_number, file_count, counter, to_json_types, api_name);
  sb << "}  // namespace " << api_name << "\n";
  sb << "}  // namespace td\n";

  CHECK(!sb.is_error());
  buf.resize(sb.as_cslice().size());
#if TD_WINDOWS
  string new_file_content;
  for (auto c : buf) {
    if (c == '\n') {
      new_file_content += '\r';
    }
    new_file_content += c;
  }
#else
  auto new_file_content = std::move(buf);
#endif
  if (new_file_content != old_file_content.as_slice()) {
    write_file(file_name, new_file_content).ensure();
  }
}

void gen_json_converter(const tl::tl_config &config, const std::string &file_name, const std::string &api_name,
                        Mode mode, int source_file_count) {
  tl::simple::Schema schema(config);
  td::FlatHashSet<std::string> to_json_types;
  gen_json_converter_file(schema, file_name, true, mode, 0, 1, to_json_types, api_name);
  for (int i = 0; i < source_file_count; i++) {
    gen_json_converter_file(schema, file_name, false, mode, i, source_file_count, to_json_types, api_name);
  }
}

}  // namespace td

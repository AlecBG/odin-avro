package main

import "core:encoding/endian"
import "core:encoding/json"
import "core:fmt"
import "core:strings"

parse_schema_from_json :: proc(json_schema: json.Value) -> Schema {
	#partial switch t in json_schema {
		case json.String: {
			return parse_schema_from_json_string(t)
		}
		case json.Array: {
            // union type
			schemas: []Schema = make([]Schema, len(t))
			for val, idx in t {
				schemas[idx] = parse_schema_from_json(t[idx])
			}
			return UnionSchema{schemas}
		}
		case json.Object: {
			return parse_schema_from_json_object(t)
		}
		case: {
		  assert(false)
		  return Null{}
		}
	}

}

parse_schema_from_json_object :: proc(json_object: json.Object) -> Schema {
	type_, type_present := json_object["type"]
	assert(type_present)
    #partial switch type in type_ {
        case json.String: {
            if type == "record" {
                fields_json, fields_present := json_object["fields"]
                assert(fields_present)
                fields_array := fields_json.(json.Array)
                fields :[]RecordField = make([]RecordField, len(fields_array))
                lookup :map[string]int = make(map[string]int, len(fields_array))
                for i in 0..<len(fields_array) {
                    field_untyped := fields_array[i]
                    field_json_val := field_untyped.(json.Object)
                    field_name_untyped, name_present := field_json_val["name"]
                    assert(name_present)
                    field_name := field_name_untyped.(json.String)
                    fields[i] = RecordField {
                        field_name,
                        parse_schema_from_json(fields_array[i]),
                        i,
                    }
                    lookup[field_name] = i
                }
                return RecordSchema{
                    fields,
                    lookup,
                }
            } else if type == "enum" {
                name_json, name_present := json_object["name"]
                assert(name_present)
                name := strings.clone(name_json.(string))
                symbols_json, symbols_present := json_object["symbols"]
                assert(symbols_present)
                symbols_array := symbols_json.(json.Array)
                symbols := make([]string, len(symbols_array))
                for i in 0..<len(symbols_array) {
                    symbols[i] = symbols_array[i].(string)
                }
                return EnumSchema{ name, symbols }
            } else if type == "array" {
                items_json, items_present := json_object["items"]
                assert(items_present)
                items_schema := parse_schema_from_json(items_json)
                return ArraySchema{ &items_schema }
            } else {
                return parse_schema_from_json_string(type)
            }
        }
        case json.Object, json.Array: {
            return parse_schema_from_json(type)
        }
        case: {
            assert(false)
            return Null{}
        }
    }
}

parse_schema_from_json_string :: proc(json_str: json.String) -> Schema {
    if json_str == "string" {
		return String{}
	} else if json_str == "int" {
		return Int{}
	} else if json_str == "long" {
	    return Long{}
	} else if json_str == "float" {
	    return Float{}
	} else if json_str == "double" {
	    return Double{}
	} else if json_str == "boolean" {
	    return Boolean{}
	} else if json_str == "bytes" {
	    return Bytes{}
	} else if json_str == "null" {
		return Null{}
	} else {
		assert(false)
		return Null{}
	}
}

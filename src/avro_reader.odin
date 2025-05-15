package main

import "core:encoding/endian"
import "core:encoding/json"
import "core:fmt"

HEADER_START := "Obj\x01"

// todo: support more codecs
SUPPORTED_CODEC :: enum {
	Null,
}

METADATA_KEY_AVRO_CODEC :: "avro.codec"
METADATA_KEY_AVRO_SCHEMA :: "avro.schema"

AvroMetadata :: struct {
	codec: SUPPORTED_CODEC,
	schema: Schema,
	sync_marker: [16]u8,
}

/*
    Plan is for this to be able to read Avro's object container files.
*/
main :: proc() {
	// todo: make this stream the file
	file_bytes := #load("../nested.avro")
	byte_pos := 0
	for byte_pos < 4 {
		assert(file_bytes[byte_pos] == HEADER_START[byte_pos])
		byte_pos += 1
	}
	metadata: AvroMetadata
	metadata, byte_pos = parse_avro_file_metadata(file_bytes, byte_pos)
	records: []Record

	fmt.println(metadata)

	records, byte_pos = parse_data_block(file_bytes, byte_pos, metadata)

	fmt.println(records)
}

parse_data_block :: proc(bytes: []u8, start_pos: int, metadata: AvroMetadata) -> ([]Record, int) {
	number_records, pos := parse_avro_long(bytes, start_pos)
	records := make([]Record, number_records)
	byte_size_l: i64
	byte_size_l, pos = parse_avro_long(bytes, pos)
	byte_size := cast(int)byte_size_l

	block_bytes :[]u8
	if metadata.codec != SUPPORTED_CODEC.Null {
		// compressed_block_bytes := bytes[pos: pos + byte_size]
		// Here is where we would do some decompression
		assert(false)
	} else {
		block_bytes = bytes[pos: pos + byte_size]
	}

	block_pos := 0
	record_idx := 0
	for block_pos < byte_size {
		records[record_idx], block_pos = parse_record(block_bytes, block_pos, metadata.schema.(RecordSchema))
		fmt.println("Parsed record ", record_idx, " val: ", records[record_idx])
		record_idx += 1
	}

	pos = pos + byte_size

	for i in 0..<len(metadata.sync_marker) {
		assert(bytes[pos + i] == metadata.sync_marker[i])
	}

	return records, pos + 16
}

parse_avro_val :: proc(bytes: []u8, pos: int, schema: Schema) -> (AvroValue, int) {
	switch s in schema {
		case RecordSchema: {
			return parse_record(bytes, pos, s)
		}
		case Boolean: {
			return parse_avro_boolean(bytes, pos)
		}
		case String: {
			return parse_avro_string(bytes, pos)
		}
		case Bytes: {
			return parse_avro_bytes(bytes, pos)
		}
		case Int: {
			return parse_avro_int(bytes, pos)
		}
		case Long: {
			return parse_avro_long(bytes, pos)
		}
		case Float: {
			return parse_avro_float(bytes, pos)
		}
		case Double: {
			return parse_avro_double(bytes, pos)
		}
		case Null: {
			return NULL_VALUE, pos
		}
		case UnionSchema: {
			schema_idx, new_pos := parse_avro_int(bytes, pos)
			return parse_avro_val(bytes, new_pos, s.schemas[schema_idx])
		}
		case: {
			assert(false)
			return nil, pos
		}
	}
}

parse_record :: proc(bytes: []u8, start_pos: int, schema: RecordSchema) -> (Record, int) {
	values: []AvroValue = make([]AvroValue, len(schema.fields))
	lookup: map[string]int = make(map[string]int, len(schema.fields))
	pos := start_pos
	for record_field, idx in schema.fields {
		lookup[record_field.name] = idx
		val: AvroValue
		val, pos = parse_avro_val(bytes, pos, record_field.schema)
		values[idx] = val
	}
	return Record{values, lookup}, pos
}

read_sync_marker :: proc(bytes: []u8, start_pos: int) -> ([16]u8, int) {
	sync_marker: [16]u8
	for i in 0..<16 {
	    sync_marker[i] = bytes[start_pos + i]
	}
	return sync_marker, start_pos + 16
}

devar_long :: proc(bytes: []u8, start_pos: int) -> ([8]u8, int) {
	output :[8]u8
	for i :u8 = 0; i < 8; i += 1 {
		if bytes[start_pos + cast(int)i] & 128 == 0 {
			output[i] = bytes[start_pos + cast(int)i] >> i
			return output, start_pos + cast(int)i + 1
		} else {
			output[i] = (bytes[start_pos + cast(int)i] & 127 >> i) | bytes[start_pos + cast(int)i + 1] << (7 - i)
		}
	}
	return output, start_pos + 9
}

devar_int :: proc(bytes: []u8, start_pos: int) -> ([4]u8, int) {
	output :[4]u8
	for i :u8 = 0; i < 4; i += 1 {
		if bytes[start_pos + cast(int)i] & 128 == 0 {
			output[i] = bytes[start_pos + cast(int)i] >> i
			return output, start_pos + cast(int)i + 1
		} else {
			output[i] = (bytes[start_pos + cast(int)i] & 127 >> i) | bytes[start_pos + cast(int)i + 1] << (7 - i)
		}
	}
	return output, start_pos + 5
}

parse_avro_int :: proc(bytes: []u8, start_pos: int) -> (AvroInt, int) {
	devarred_bytes, pos := devar_int(bytes, start_pos)
	devarred_int :i32 = transmute(i32)devarred_bytes
	output :i32 = (devarred_int >> 1) ~ -(devarred_int & 1)
	return output, pos
}

parse_avro_long :: proc(bytes: []u8, start_pos: int) -> (i64, int) {
	devarred_bytes, pos := devar_long(bytes, start_pos)
	devarred_int :i64 = transmute(i64)devarred_bytes
	return (devarred_int >> 1) ~ -(devarred_int & 1), pos
}

parse_avro_float :: proc(bytes: []u8, start_pos: int) -> (AvroFloat, int) {
	val, ok := endian.get_f32(bytes[start_pos: start_pos + 4], .Little)
	assert(ok)
	return val, start_pos + 4
}

parse_avro_double :: proc(bytes: []u8, start_pos: int) -> (AvroDouble, int) {
	val, ok := endian.get_f64(bytes[start_pos: start_pos + 8], .Little)
	assert(ok)
	return val, start_pos + 8
}

parse_avro_bytes :: proc(bytes: []u8, start_pos: int) -> ([]u8, int) {
	length_l, pos := parse_avro_long(bytes, start_pos)
	length := cast(int)length_l
	output := make([]u8, length)
	for i in 0..<length {
		output[i] = bytes[pos + i]
	}
	return output, pos + length
}

parse_avro_string :: proc(bytes: []u8, start_pos: int) -> (AvroString, int) {
	bs, pos := parse_avro_bytes(bytes, start_pos)
	return transmute(string)bs, pos
}

parse_avro_boolean :: proc(bytes: []u8, start_pos: int) -> (AvroBoolean, int) {
	return bytes[start_pos] == 1, start_pos + 1
}

parse_avro_codec :: proc(codec_bytes: []u8) -> SUPPORTED_CODEC {
	if transmute(string)codec_bytes == "null"  {
		return SUPPORTED_CODEC.Null
	} else {
		// todo: improve error handling!
		assert(false)
		return SUPPORTED_CODEC.Null
	}
}

parse_avro_file_metadata :: proc(bytes: []u8, start_pos: int) -> (AvroMetadata, int) {
	num_entries, pos := parse_avro_long(bytes, start_pos)
	codec: SUPPORTED_CODEC
	schema: json.Value
	json_schema_err: json.Error
	for _ in 0..<num_entries {
		key: string
		key, pos = parse_avro_string(bytes, pos)
		if key == METADATA_KEY_AVRO_CODEC {
			bs: []u8
			bs, pos = parse_avro_bytes(bytes, pos)
			codec = parse_avro_codec(bs)
		} else if key == METADATA_KEY_AVRO_SCHEMA {
			schema_bytes: []u8
			schema_bytes, pos = parse_avro_bytes(bytes, pos)
			schema, json_schema_err = json.parse(schema_bytes)
			fmt.println("schema json\n", schema)
			if json_schema_err != nil {
				// todo: bubble up errors
				assert(false)
			}
		}
	}

	if codec != SUPPORTED_CODEC.Null {
		// todo: bubble up errors
		assert(false)
	}
	if schema == nil {
		// todo: bubble up errors
		assert(false)
	}

	end_of_map: i64
	end_of_map, pos = parse_avro_long(bytes, pos)
	assert(end_of_map == 0)

	sync_marker: [16]u8
	sync_marker, pos = read_sync_marker(bytes, pos)

	return AvroMetadata{codec, parse_schema(schema), sync_marker}, pos
}

parse_with_string_type :: proc(t: string, object: json.Object) -> Schema {
	if t == "record" {
		fields_json, fields_present := object["fields"]
		assert(fields_present)
		fields_array := fields_json.(json.Array)
		fields :[]RecordField = make([]RecordField, len(fields_array))
		lookup :map[string]int = make(map[string]int, len(fields_array))
		for i := 0; i < len(fields_array); i += 1 {
			field_untyped := fields_array[i]
			field_json_val := field_untyped.(json.Object)
			field_name_untyped, name_present := field_json_val["name"]
			assert(name_present)
			field_name := field_name_untyped.(json.String)
			fields[i] = RecordField {
				field_name,
				parse_schema(fields_array[i]),
				i,
			}
			lookup[field_name] = i
		}
		return RecordSchema{
			fields,
			lookup,
		}
	} else if t == "string" {
		return String{}
	} else if t == "int" {
		return Int{}
	} else if t == "long" {
	    return Long{}
	} else if t == "float" {
	    return Float{}
	} else if t == "double" {
	    return Double{}
	} else if t == "boolean" {
	    return Boolean{}
	} else if t == "bytes" {
	    return Bytes{}
	}else if t == "null" {
		return Null{}
	} else {
		assert(false)
		return Null{}
	}
}

parse_schema :: proc(json_schema: json.Value) -> Schema {
	object := json_schema.(json.Object)
	type, type_present := object["type"]
	assert(type_present)
	#partial switch t in type {
		case json.String: {
			return parse_with_string_type(t, object)
		}
		case json.Array: {
			schemas: []Schema = make([]Schema, len(t))
			for val, idx in t {
				val_str := val.(json.String)
				if val_str == "string" {
					schemas[idx] = String{}
				} else if val_str == "null" {
					schemas[idx] = Null{}
				} else if val_str == "int" {
					schemas[idx] = Int{}
				} else {
					assert(false)
				}
			}
			return UnionSchema{schemas}
		}
		case json.Object: {
			return parse_schema(t)
		}
		case: {
		  assert(false)
		  return Null{}
		}
	}

}

package main

import "core:encoding/endian"
import "core:encoding/json"
import "core:fmt"
import "core:strings"

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
	records: []AvroRecord

	fmt.println(metadata)

	records, byte_pos = parse_data_block(file_bytes, byte_pos, metadata)

	fmt.println(records)
}

parse_data_block :: proc(bytes: []u8, start_pos: int, metadata: AvroMetadata) -> ([]AvroRecord, int) {
	number_records, pos := parse_avro_long(bytes, start_pos)
	records := make([]AvroRecord, number_records)
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
		case RecordSchema: {
			return parse_record(bytes, pos, s)
		}
		case ArraySchema: {
			return parse_avro_array(bytes, pos, s)
		}
		case EnumSchema: {
			return parse_avro_enum(bytes, pos, s)
		}
		case: {
			assert(false)
			return nil, pos
		}
	}
}

parse_record :: proc(bytes: []u8, start_pos: int, schema: RecordSchema) -> (AvroRecord, int) {
	values: []AvroValue = make([]AvroValue, len(schema.fields))
	lookup: map[string]int = make(map[string]int, len(schema.fields))
	pos := start_pos
	for record_field, idx in schema.fields {
		lookup[record_field.name] = idx
		val: AvroValue
		val, pos = parse_avro_val(bytes, pos, record_field.schema)
		values[idx] = val
	}
	return AvroRecord{values, lookup}, pos
}

parse_avro_array :: proc(bytes: []u8, start_pos: int, array_schema: ArraySchema) -> (AvroArray, int) {
	values: [dynamic]AvroValue
	total_length :int= 0

	length, pos := parse_avro_long(bytes, start_pos)
	// length 0 signifies end of blocks.
	for length != 0 {
		if length < 0 {
			// See spec https://avro.apache.org/docs/1.11.1/specification/#complex-types-1
			//  if length negative, next value is long signifying block byte length
			length = -length
			num_bytes: i64
			num_bytes, pos = parse_avro_long(bytes, pos)
		}
		total_length += cast(int)length
	
		resize(&values, total_length)
		for i in 0..<length {
			val: AvroValue
			val, pos = parse_avro_val(bytes, pos, array_schema.items_schema^)
			append(&values, val)
		}
		length, pos = parse_avro_long(bytes, start_pos)
	}
	return values, pos
}

parse_avro_enum :: proc(bytes: []u8, start_pos: int, schema: EnumSchema) -> (AvroEnum, int) {
	symbol_idx, pos := parse_avro_int(bytes, start_pos)
	return strings.clone(schema.symbols[symbol_idx]), pos
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
			fmt.println("schema json\n", schema, "\n")
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

	return AvroMetadata{codec, parse_schema_from_json(schema), sync_marker}, pos
}

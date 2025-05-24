package avro

import "core:encoding/endian"
import "core:strings"

parse_avro_val :: proc(bytes: []u8, pos: int, schema: ^Schema) -> (AvroValue, int) {
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
	
		for _ in 0..<length {
			val: AvroValue
			val, pos = parse_avro_val(bytes, pos, array_schema.items_schema)
			append(&values, val)
		}
		length, pos = parse_avro_long(bytes, pos)
	}
	return values, pos
}

parse_avro_enum :: proc(bytes: []u8, start_pos: int, schema: EnumSchema) -> (AvroEnum, int) {
	symbol_idx, pos := parse_avro_int(bytes, start_pos)
	return strings.clone(schema.symbols[symbol_idx]), pos
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

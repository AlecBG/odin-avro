package avro

import "core:encoding/json"

AvroObjectContainerFile :: struct {
    metadata: AvroMetadata,
    records: []AvroRecord,
}

read_object_container_file :: proc(bytes: []u8) -> AvroObjectContainerFile {
    byte_pos := 0
	for byte_pos < 4 {
		assert(bytes[byte_pos] == HEADER_START[byte_pos])
		byte_pos += 1
	}
	metadata: AvroMetadata
	metadata, byte_pos = parse_avro_file_metadata(bytes, byte_pos)
	records: []AvroRecord

	records, byte_pos = parse_data_block(bytes, byte_pos, metadata)

    return AvroObjectContainerFile{ metadata, records }
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
		record_idx += 1
	}

	pos = pos + byte_size

	for i in 0..<len(metadata.sync_marker) {
		assert(bytes[pos + i] == metadata.sync_marker[i])
	}

	return records, pos + 16
}

read_sync_marker :: proc(bytes: []u8, start_pos: int) -> ([16]u8, int) {
	sync_marker: [16]u8
	for i in 0..<16 {
	    sync_marker[i] = bytes[start_pos + i]
	}
	return sync_marker, start_pos + 16
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
		defer delete(key)
		if key == METADATA_KEY_AVRO_CODEC {
			bs: []u8
			bs, pos = parse_avro_bytes(bytes, pos)
			codec = parse_avro_codec(bs)
			delete(bs)
		} else if key == METADATA_KEY_AVRO_SCHEMA {
			schema_bytes: []u8
			schema_bytes, pos = parse_avro_bytes(bytes, pos)
			schema, json_schema_err = json.parse(schema_bytes)
			delete(schema_bytes)
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

	avro_schema := parse_schema_from_json(schema)
	json.destroy_value(schema)

	return AvroMetadata{codec, avro_schema, sync_marker}, pos
}

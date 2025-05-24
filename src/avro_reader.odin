package avro

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

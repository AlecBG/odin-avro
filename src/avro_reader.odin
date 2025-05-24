package avro

import "core:fmt"
import "core:os"

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
	file_bytes, error := os.read_entire_file_from_filename_or_err("./nested.avro", context.allocator)
	if error != nil {
		fmt.println("error reading file", error)
		assert(false)
	}
	defer delete(file_bytes, context.allocator)

	object_container := read_object_container_file(file_bytes)

	fmt.println("Schema\n", object_container.metadata.schema, "\n=====================\nRecords", sep="")
	
	for record in object_container.records {
		fmt.println(record)
	}

	destroy_schema(&object_container.metadata.schema)

	for &record in object_container.records {
		destroy_record(&record)
	}
	delete(object_container.records)

}

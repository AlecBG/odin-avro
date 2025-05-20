package main


// This is missing a lot of the functionality of avro schemas.
Boolean :: struct{}

Bytes :: struct{}

String :: struct{}

Int :: struct{}

Long :: struct{}

Float :: struct{}

Double :: struct{}

Null :: struct {}

RecordSchema :: struct {
    fields: []RecordField,
    // map field name to position in vec
    lookup: map[string]int,
}

RecordField :: struct {
    name: string,
    schema: Schema,
    position: int,
}

ArraySchema :: struct {
    items_schema: ^Schema,
}

UnionSchema :: struct {
    schemas: []Schema,
}

EnumSchema :: struct {
    name: string,
    symbols: []string,
}

Schema :: union {
    // type 'boolean'
    Boolean,
    // type 'string'
    String,
    // type 'bytes'
    Bytes,
    // type 'int'
    Int,
    // type 'long'
    Long,
    // type 'float'
    Float,
    // type 'double'
    Double,
    // type 'null'
    Null,
    // type 'array'
    ArraySchema,
    // type 'enum'
    EnumSchema,
    // type 'record'
    RecordSchema,
    // type 'union'
    UnionSchema,
}

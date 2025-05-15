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
    // type 'record'
    RecordSchema,
    // type 'union'
    UnionSchema,
    Null,
}

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

UnionSchema :: struct {
    schemas: []Schema,
}

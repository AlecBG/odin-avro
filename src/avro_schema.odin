package main


// This is missing a lot of the functionality of avro schemas.
String :: struct{}

Int :: struct{}

Null :: struct {}

Schema :: union {
    // type 'record'
    RecordSchema,
    // type 'string'
    String,
    // type 'int'
    Int,
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
    position: int
}

UnionSchema :: struct {
    schemas: []Schema
}

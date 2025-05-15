package main

AvroValue :: union {
    Record,
    NullValue,
    AvroBoolean,
    AvroInt,
    AvroLong,
    AvroFloat,
    AvroDouble,
    AvroString,
    AvroBytes,
}

NullValue :: struct {
}

NULL_VALUE :: NullValue{}

Record :: struct {
	values: []AvroValue,
	lookup: map[string]int,
}

AvroBoolean :: bool

AvroString :: string

AvroInt :: i32

AvroLong :: i64

AvroBytes :: []u8

AvroFloat :: f32

AvroDouble :: f64

package main

AvroValue :: union {
    AvroRecord,
    AvroArray,
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

AvroRecord :: struct {
	values: []AvroValue,
	lookup: map[string]int,
}

AvroArray :: [dynamic]AvroValue

AvroBoolean :: bool

AvroString :: string

AvroEnum :: string

AvroInt :: i32

AvroLong :: i64

AvroBytes :: []u8

AvroFloat :: f32

AvroDouble :: f64

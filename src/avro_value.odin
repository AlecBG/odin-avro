package main

AvroValue :: union {
    Record,
    NullValue,
    AvroString,
    AvroInt,
}

NullValue :: struct {
}

NULL_VALUE :: NullValue{}

Record :: struct {
	values: []AvroValue,
	lookup: map[string]int,
}

AvroString :: string

AvroInt :: i32

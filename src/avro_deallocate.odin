package avro

// https://www.youtube.com/watch?v=eSqexFg74F8&t=112s

destroy_schema :: proc(schema: ^Schema) {
    switch s in schema {
        case Boolean, String, Bytes, Int, Long, Float, Double, Null: {
            // deallocation a no-op for these cases
        }
        case ArraySchema: {
            destroy_schema(s.items_schema)
            free(s.items_schema)
        }
        case EnumSchema: {
            delete(s.name)
            for symbol in s.symbols {
                delete(symbol)
            }
            delete(s.symbols)
        }
        case RecordSchema: {
            for record_field in s.fields {
                delete(record_field.name)
                destroy_schema(record_field.schema)
                free(record_field.schema)
            }
            delete(s.fields)
            delete(s.lookup)
        }
        case UnionSchema: {
            for schema in s.schemas {
                destroy_schema(schema)
            }
            delete(s.schemas)
        }
    }
}

destroy_value :: proc(value: ^AvroValue) {
    switch &v in value {
        case NullValue, AvroBoolean, AvroInt, AvroLong, AvroFloat, AvroDouble: {
            // no-op
        }
        case AvroRecord: {
            destroy_record(&v)
        }
        case AvroArray: {
            for &val in v {
                destroy_value(&val)
            }
            delete(v)
        }
        case AvroString: {
            delete(v)
        }
        case AvroBytes: {
            delete(v)
        }
    }
}

destroy_record :: proc(record: ^AvroRecord) {
    for &val in record.values {
        destroy_value(&val)
    }
    delete(record.values)
    delete(record.lookup)
}

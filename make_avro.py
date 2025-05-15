import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("user.avsc", "rb").read())

writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()


nested_schema = avro.schema.make_avsc_object(
    {
        "namespace": "example.avro",
        "name": "MySchema",
        "type": "record",
        "fields": [
            {
                "type": "string",
                "name": "my-string",
            },
            {
                "name": "nested",
                "type": {
                    "type": "record",
                    "name": "nested_schema",
                    "fields": [
                        {
                            "type": "int",
                            "name": "nested-int",
                        },
                        {
                            "type": "string",
                            "name": "nested-string",
                        },
                        {
                            "type": "boolean",
                            "name": "oooh_look_at_me_im_a_boolean",
                        },
                        {
                            "type": "bytes",
                            "name": "bs",
                        },
                        {"type": "float", "name": "f"},
                        {"type": "long", "name": "l"},
                        {"type": "double", "name": "d"},
                    ],
                },
            },
        ],
    }
)

writer = DataFileWriter(open("nested.avro", "wb"), DatumWriter(), nested_schema)
writer.append(
    {
        "my-string": "Alyssa",
        "nested": {
            "nested-int": 256,
            "nested-string": "a value",
            "oooh_look_at_me_im_a_boolean": True,
            "bs": b"\x00\x01\x02\x03\x04",
            "f": 1.23,
            "l": 124,
            "d": 421.412414124213211
        },
    }
)
writer.append(
    {
        "my-string": "Benjamin",
        "nested": {
            "nested-int": 124,
            "nested-string": "a different value",
            "oooh_look_at_me_im_a_boolean": True,
            "bs": b"\x05\x06\x07\x08\x09",
            "f": 3.14159,
            "l": 124127817282124,
            "d": 3.1415926535898
        },
    }
)
writer.append(
    {
        "my-string": "Caravaggio",
        "nested": {
            "nested-int": 69,
            "nested-string": "#œœ≈kmqmo^µ∑≈œ≈µ\x07",
            "oooh_look_at_me_im_a_boolean": True,
            "bs": b"\x10\x11\x12\x13\x14",
            "f": -12321.1241212,
            "l": -(2 ** (40)),
            "d": 0.57721566490153286060651209008240243
        },
    }
)
writer.close()

reader = DataFileReader(open("nested.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()

reader = DataFileReader(open("users.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()

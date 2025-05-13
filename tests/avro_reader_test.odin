package tests

import "core:testing"
import main "../src/"

AvroLongTestPair :: struct {
    input_bytes: []u8,
    input_start: int,
    expected_output: i64,
    expected_output_pos: int,
}

@(test)
test_parse_avro_long :: proc(t: ^testing.T) {
    test_pairs : []AvroLongTestPair = {
        AvroLongTestPair{{0}, 0, 0, 1},
        AvroLongTestPair{{1}, 0, -1, 1},
        AvroLongTestPair{{2}, 0, 1, 1},
        AvroLongTestPair{{3}, 0, -2, 1},
        AvroLongTestPair{{4}, 0, 2, 1},
        AvroLongTestPair{{5}, 0, -3, 1},
        AvroLongTestPair{{6}, 0, 3, 1},
        // ignore junk at start
        AvroLongTestPair{{123, 41, 255, 0}, 3, 0, 4},
        AvroLongTestPair{{123, 41, 255, 1}, 3, -1, 4},
        AvroLongTestPair{{123, 41, 255, 2}, 3, 1, 4},
        AvroLongTestPair{{123, 41, 255, 3}, 3, -2, 4},
        AvroLongTestPair{{123, 41, 255, 4}, 3, 2, 4},
        AvroLongTestPair{{123, 41, 255, 5}, 3, -3, 4},
        AvroLongTestPair{{123, 41, 255, 6}, 3, 3, 4},
        AvroLongTestPair{{123, 41, 255, 99}, 3, -50, 4},
        AvroLongTestPair{{123, 41, 255, 100}, 3, 50, 4},
        AvroLongTestPair{{43, 127}, 1, -64, 2},
        // More than one byte (ignore first)
        AvroLongTestPair{{43, 128, 1}, 1, 64, 3},
        AvroLongTestPair{{43, 129, 1}, 1, -65, 3},
        AvroLongTestPair{{43, 130, 1}, 1, 65, 3},
        AvroLongTestPair{{43, 131, 1}, 1, -66, 3},
        AvroLongTestPair{{43, 200, 200, 12}, 1, 102948, 4},
    }

    for test_pair in test_pairs {
        output, output_pos := main.parse_avro_long(test_pair.input_bytes, test_pair.input_start)
        testing.expectf(
            t,
            output == test_pair.expected_output,
            "Expected parsed long value %d got %d",
            test_pair.expected_output,
            output,
        )
        testing.expectf(
            t,
            output_pos == test_pair.expected_output_pos,
            "Expected output position %d got %d",
            test_pair.expected_output_pos,
            output_pos,
        )
    }
}


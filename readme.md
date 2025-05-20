# Avro reader

I'm writing a reader of Avro files in the Odin language. Mainly for fun.

Very much a work in progress and I make no promises about finishing this.

## todo

- [ ] read avro object with complex types
    - [ ] arrays
    - [x] nested records
    - [ ] maps
    - [x] enums
    - [ ] fixed
    - [x] unions
- [x] read avro object containing any primitive types
- [ ] logical types (such as all timestamps)
- [ ] aliases
- [ ] default values
- [ ] Add proper error handling
- [ ] Stream the file instead of reading all at once to allow reading very large files
- [ ] Write out a proper test suite
- [ ] Improve how memory is stored
- [ ] Add some proper CI to run tests (and linting if odin has any good linting...)
- [ ] write avro

What would be really fun would be after finishing avro, writing a parquet reader and possibly even an Iceberg table reader...

package io.vonsowic

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

private val nonNullString = SchemaBuilder.builder().stringType()
private val nullableString =
    SchemaBuilder
        .builder()
        .unionOf()
        .nullType()
        .and()
        .stringType()
        .endUnion()

val PeopleSchema: Schema =
    SchemaBuilder.builder()
        .record("person")
        .fields()
        .name("id")
        .type(nonNullString)
        .noDefault()
        .name("firstName")
        .type(nonNullString)
        .noDefault()
        .name("lastName")
        .type(nonNullString)
        .noDefault()
        .name("birthDate")
        .type(SchemaBuilder.builder().longType())
        .withDefault(0L)
        .name("favouriteAnimal")
        .type(nullableString)
        .withDefault(null)
        .endRecord()
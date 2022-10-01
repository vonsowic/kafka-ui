package io.vonsowic

import net.datafaker.Faker
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import java.util.*

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

fun randomPerson(): GenericData.Record =
    with(Faker.instance()) {
        GenericRecordBuilder(PeopleSchema)
            .set("id", UUID.randomUUID().toString())
            .set("firstName", name().firstName())
            .set("lastName", name().lastName())
            .set("birthDate", date().birthday().toInstant().toEpochMilli())
            .set("favouriteAnimal", animal().name())
            .build()
    }

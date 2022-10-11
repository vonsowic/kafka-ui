package io.vonsowic

import io.vonsowic.test.avro.*
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

val PeopleSchemaV2: Schema =
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
        .name("aFloat")
        .type(SchemaBuilder.builder().floatType())
        .noDefault()
        .name("aDouble")
        .type(SchemaBuilder.builder().doubleType())
        .noDefault()
        .name("aBoolean")
        .type(SchemaBuilder.builder().booleanType())
        .noDefault()
        .name("aInt")
        .type(SchemaBuilder.builder().intType())
        .noDefault()
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

fun randomAddress(personId: String = UUID.randomUUID().toString()): Address =
    Address.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setPersonId(personId)
        .setAddress(Faker.instance().address().fullAddress())
        .build()

fun randomPersonV2(): GenericData.Record =
    with(Faker.instance()) {
        GenericRecordBuilder(PeopleSchemaV2)
            .set("id", UUID.randomUUID().toString())
            .set("firstName", name().firstName())
            .set("lastName", name().lastName())
            .set("birthDate", date().birthday().toInstant().toEpochMilli())
            .set("favouriteAnimal", animal().name())
            .set("aFloat", random().nextFloat())
            .set("aDouble", random().nextDouble())
            .set("aBoolean", random().nextBoolean())
            .set("aInt", random().nextInt())
            .build()
    }

fun randomUberAvro(): UberAvro =
    with(Faker.instance()) {
        UberAvro
            .newBuilder()
            .setANull(null)
            .setABoolean(random().nextBoolean())
            .setAInt(random().nextInt())
            .setALong(random().nextLong())
            .setAFloat(random().nextFloat())
            .setADouble(random().nextDouble())
            .setARecord(
                TestNestedRecord
                    .newBuilder()
                    .setNestedField(UUID.randomUUID().toString())
                    .build()
            )
            .setAEnum(TestEnum.values()[random().nextInt(0, 2)])
            .setAStringArray((0..(random().nextInt(3, 20))).map { "${name().firstName()} ${name().lastName()}" })
            .setALongMap(
                mapOf(
                    "key1" to random().nextLong(),
                    "key2" to random().nextLong(),
                    "key3" to random().nextLong()
                )
            )
            .setAFixed(TestFixed(UUID.randomUUID().toString().substring(0, 16).toByteArray()))
            .setAString(UUID.randomUUID().toString())
            .build()
    }

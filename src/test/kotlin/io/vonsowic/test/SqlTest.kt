package io.vonsowic.test

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.micronaut.http.HttpStatus
import io.vonsowic.*
import jakarta.inject.Inject
import net.datafaker.Faker
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.*

const val PEOPLE_TOPIC = "peopletest"

@IntegrationTest
class SqlTest(
    @Inject
    private val httpClient: AppClient
) {

    // ExceptionHandler does not work
    @Disabled
    @Test
    fun `should return bad request if the topic does not exist`() {
        val res = httpClient.expectError { sql(SqlStatementReq("SELECT * FROM people")) }
        assertThat(res.status).isEqualTo(HttpStatus.BAD_REQUEST)
    }

    @Topic(PEOPLE_TOPIC)
    @Test
    fun `should return rows representing Kafka events`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, GenericData.Record>
    ) {
        val personId = UUID.randomUUID().toString()
        val person =
            GenericRecordBuilder(PeopleSchema)
                .set("id", personId)
                .set("firstName", Faker.instance().name().firstName())
                .set("lastName", Faker.instance().name().lastName())
                .set("birthDate", Faker.instance().date().birthday().toInstant().toEpochMilli())
                .set("favouriteAnimal", Faker.instance().animal().name())
                .build()
        val res = producer.send(ProducerRecord(PEOPLE_TOPIC, personId, person)).get()

        val rows =
            httpClient
                .expectStatus<List<SqlStatementRow>>(HttpStatus.OK) {
                    sql(SqlStatementReq("SELECT * FROM $PEOPLE_TOPIC"))
                }
                .body()

        assertThat(rows).hasSize(2)
        assertThat(rows[0])
            .contains(
                "ID",
                "FIRSTNAME",
                "LASTNAME",
                "BIRTHDATE",
                "FAVOURITEANIMAL",
            )
        assertThat(rows[1])
            .contains(
                person["id"],
                person["firstName"],
                person["lastName"],
                person["birthDate"],
                person["favouriteAnimal"],
            )
    }
}
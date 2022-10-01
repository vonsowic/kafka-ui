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

    @Topic("sqltestpeopletest1")
    @Test
    fun `should return rows representing Kafka events`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, GenericData.Record>
    ) {
        val person = randomPerson()
        producer.send(ProducerRecord("sqltestpeopletest1", person.get("id") as String, person)).get()

        val rows =
            httpClient
                .expectStatus<List<SqlStatementRow>>(HttpStatus.OK) {
                    sql(SqlStatementReq("SELECT * FROM sqltestpeopletest1"))
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

    @Topic("sqltestpeopletest2")
    @Test
    fun `should mirror Kafka topic only once - second call should not trigger second Kafka subscription`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, GenericData.Record>
    ) {
        val person = randomPerson()
        producer.send(ProducerRecord("sqltestpeopletest2", person.get("id") as String, person)).get()

        httpClient
            .expectStatus<List<SqlStatementRow>>(HttpStatus.OK) {
                sql(SqlStatementReq("SELECT * FROM sqltestpeopletest2"))
            }

        // execute for the second time
        val rows =
            httpClient
                .expectStatus<List<SqlStatementRow>>(HttpStatus.OK) {
                    sql(SqlStatementReq("SELECT * FROM sqltestpeopletest2"))
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
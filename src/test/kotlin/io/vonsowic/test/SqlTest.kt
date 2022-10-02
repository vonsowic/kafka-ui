package io.vonsowic.test

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.micronaut.http.HttpStatus
import io.vonsowic.*
import jakarta.inject.Inject
import net.datafaker.Faker
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
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

    @Topic("sql-test-people-4")
    @Topic("sql-test-address-4")
    @Test
    fun `test with sql join`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, GenericRecord>
    ) {
        val person = randomPerson()
        val personId = person.get("id") as String
        producer.send(ProducerRecord("sql-test-people-4", personId, person))
        val address = randomAddress(personId)
        producer.send(ProducerRecord("sql-test-address-4", address.id, address))
        producer.flush()

        val rows =
            httpClient
                .expectStatus<List<SqlStatementRow>>(HttpStatus.OK) {
                    sql(SqlStatementReq("SELECT *, a.ID as ADDRESSID FROM \"sql-test-people-4\" p JOIN \"sql-test-address-4\" a ON p.ID = a.PERSONID"))
                }
                .body()

        assertThat(rows).hasSize(1)
        assertThat(rows[0])
            .containsEntry("ID", personId)
            .containsEntry("FIRSTNAME", person["firstName"])
            .containsEntry("LASTNAME", person["lastName"])
            .containsEntry("BIRTHDATE", person["birthDate"])
            .containsEntry("ADDRESSID", address.id)
            .containsEntry("PERSONID", personId)
            .containsEntry("ADDRESS", address.address)
    }

    @Topic("sql-test-people3")
    @Test
    fun `test all types`(
        @ProducerOptions(valueSerializer = KafkaAvroSerializer::class)
        producer: Producer<String, GenericData.Record>
    ) {
        val person = randomPersonV2()
        producer.send(ProducerRecord("sql-test-people3", person.get("id") as String, person)).get()
        val rows =
            httpClient
                .expectStatus<List<SqlStatementRow>>(HttpStatus.OK) {
                    sql(SqlStatementReq("SELECT * FROM \"sql-test-people3\""))
                }
                .body()

        assertThat(rows).hasSize(1)
        assertThat(rows[0])
            .containsEntry("ID", person["id"])
            .containsEntry("FIRSTNAME", person["firstName"])
            .containsEntry("LASTNAME", person["lastName"])
            .containsEntry("BIRTHDATE", person["birthDate"])
            .containsEntry("FAVOURITEANIMAL", person["favouriteAnimal"])
            .containsEntry("AFLOAT", person["aFloat"].toString().toDouble())
            .containsEntry("ADOUBLE", person["aDouble"])
            .containsEntry("ABOOLEAN", person["aBoolean"])
            .containsEntry("AINT", person["aInt"])
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
                    sql(SqlStatementReq("SELECT * FROM \"sqltestpeopletest1\""))
                }
                .body()

        assertThat(rows).hasSize(1)
        assertThat(rows[0])
            .containsEntry("ID", person["id"])
            .containsEntry("FIRSTNAME", person["firstName"])
            .containsEntry("LASTNAME", person["lastName"])
            .containsEntry("BIRTHDATE", person["birthDate"])
            .containsEntry("FAVOURITEANIMAL", person["favouriteAnimal"])
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
                sql(SqlStatementReq("SELECT * FROM \"sqltestpeopletest2\""))
            }

        // execute for the second time
        val rows =
            httpClient
                .expectStatus<List<SqlStatementRow>>(HttpStatus.OK) {
                    sql(SqlStatementReq("SELECT * FROM \"sqltestpeopletest2\""))
                }
                .body()

        assertThat(rows).hasSize(1)
        assertThat(rows[0])
            .containsEntry("ID", person["id"])
            .containsEntry("FIRSTNAME", person["firstName"])
            .containsEntry("LASTNAME", person["lastName"])
            .containsEntry("BIRTHDATE", person["birthDate"])
            .containsEntry("FAVOURITEANIMAL", person["favouriteAnimal"])
    }

}
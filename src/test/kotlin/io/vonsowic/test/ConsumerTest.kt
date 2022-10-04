package io.vonsowic.test

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.vonsowic.*
import net.datafaker.Faker
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.util.*

@IntegrationTest
class ConsumerTest(
    private val httpClient: AppClient,
    @Client("/api")
    private val rawHttpClient: HttpClient
) {

    @Topic("consumer-test-5-1", partitions = 2)
    @Topic("consumer-test-5-2", partitions = 1)
    @Test
    fun `should fetch kafka events using selected range`(
        @ProducerOptions
        producer: Producer<String?, String?>,
    ) {
        repeat(20) {
            producer.send(ProducerRecord("consumer-test-5-1", 0, null, null))
        }
        repeat(10) {
            producer.send(ProducerRecord("consumer-test-5-1", 1, null, null))
            producer.send(ProducerRecord("consumer-test-5-2", 0, null, null))
        }
        producer.flush()

        val req = HttpRequest.GET<List<KafkaEvent>>("/events")
            .apply {
                with(parameters) {
                    add("t0", "consumer-test-5-1")
                    add("t1", "consumer-test-5-2")

                    // from offset 10 to 12 (including start and end range)
                    add("t0p0e", "10")
                    add("t0p0l", "12")

                    // from the last event (including last)
                    add("t0p1e", "9")

                    // to the first event only (including first)
                    add("t1p0l", "0")
                }
            }

        Mono.from(rawHttpClient.retrieve(req, Argument.listOf(KafkaEvent::class.java)))
            .block()!!
            .also {
                assertThat(it).hasSize(5)
            }
            .apply {

            }
    }

    @Topic("consumer-test-1")
    @Test
    fun `should fetch empty list`() {
        with(httpClient.fetchEvents("consumer-test-1")) {
            assertThat(status).isEqualTo(HttpStatus.OK)
            assertThat(body()).isEmpty()
        }
    }

    @Topic("consumer-test-2")
    @Test
    fun `should fetch Kafka event with null key and null value`(
        @ProducerOptions
        producer: Producer<String, String?>
    ) {
        producer.send(ProducerRecord("consumer-test-2", null, null)).get()

        httpClient.fetchEvents("consumer-test-2")
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
                assertThat(body()).hasSize(1)
            }
            .let { it.body()[0] }
            .apply {
                assertThat(key.type).isEqualTo(KafkaEventPartType.NIL)
                assertThat(key.data).isEqualTo(0)
                assertThat(value.type).isEqualTo(KafkaEventPartType.NIL)
                assertThat(value.data).isEqualTo(0)
            }
    }

    @Topic("consumer-test-3")
    @Test
    fun `should fetch all Kafka events with key string and value string`(
        @ProducerOptions
        producer: Producer<String, String>
    ) {
        val testKey = "key-${UUID.randomUUID()}"
        val testValue = "value-${UUID.randomUUID()}"
        producer.send(ProducerRecord("consumer-test-3", testKey, testValue)).get()

        httpClient.fetchEvents("consumer-test-3")
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
                assertThat(body()).hasSize(1)
            }
            .let { it.body()[0] }
            .apply {
                assertThat(key.type).isEqualTo(KafkaEventPartType.STRING)
                assertThat(key.data).isEqualTo(testKey)
                assertThat(value.type).isEqualTo(KafkaEventPartType.STRING)
                assertThat(value.data).isEqualTo(testValue)
            }
    }

    @Topic("consumer-test-4")
    @Test
    fun `should fetch all Kafka events with key avro and value avro`(
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
                .set("favouriteAnimal", null)
                .build()

        producer.send(ProducerRecord("consumer-test-4", personId, person)).get()

        httpClient.fetchEvents("consumer-test-4")
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
                assertThat(body()).hasSize(1)
            }
            .let { it.body()[0] }
            .apply {
                assertThat(key.type).isEqualTo(KafkaEventPartType.STRING)
                assertThat(key.data).isEqualTo(personId)
                assertThat(value.type).isEqualTo(KafkaEventPartType.AVRO)
                assertThat(value.data as Map<String, Any?>)
                    .hasSize(4)
                    .containsEntry("id", person["id"])
                    .containsEntry("firstName", person["firstName"])
                    .containsEntry("lastName", person["lastName"])
                    .containsEntry("birthDate", person["birthDate"])
            }
    }
}

package io.vonsowic.integration

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.sse.SseClient
import io.vonsowic.*
import io.vonsowic.test.avro.Id
import net.datafaker.Faker
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.Test
import org.testcontainers.shaded.org.awaitility.Awaitility.await
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URL
import java.time.Duration
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
                accept(MediaType.APPLICATION_JSON)
                with(parameters) {
                    add("t0", "consumer-test-5-1")
                    add("t1", "consumer-test-5-2")

                    // from offset 10 to 12 (including start and excluding end range)
                    add("t0p0e", "10")
                    add("t0p0l", "13")

                    // from the last event (excluding last)
                    add("t0p1e", "9")

                    // to the first event only (including first)
                    add("t1p0l", "1")
                }
            }

        Mono.from(rawHttpClient.retrieve(req, Argument.listOf(KafkaEvent::class.java)))
            .block()!!
            .also {
                assertSoftly { softly ->
                    with(softly) {
                        assertThat(it).hasSize(5)
                        val countsPerTopicPartition =
                            it.groupBy { TopicPartition(it.topic, it.partition) }
                                .entries
                                .associate { it.key to it.value.size }
                        assertThat(countsPerTopicPartition[TopicPartition("consumer-test-5-1", 0)]).isEqualTo(3)
                        assertThat(countsPerTopicPartition[TopicPartition("consumer-test-5-1", 1)]).isEqualTo(1)
                        assertThat(countsPerTopicPartition[TopicPartition("consumer-test-5-2", 0)]).isEqualTo(1)
                    }
                }
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

    @Topic("consumer-test-5")
    @Test
    fun `should fetch all Kafka events with key Id and value UberAvro`(
        @ProducerOptions(
            keySerializer = KafkaAvroSerializer::class,
            valueSerializer = KafkaAvroSerializer::class
        )
        producer: Producer<SpecificRecord, SpecificRecord>
    ) {
        val id = Id(UUID.randomUUID().toString())
        val uberAvro = randomUberAvro()
        producer.send(ProducerRecord("consumer-test-5", id, uberAvro)).get()


        httpClient.fetchEvents("consumer-test-5")
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
                assertThat(body()).hasSize(1)
            }
            .let { it.body()[0] }
            .apply {
                assertSoftly { softly ->
                    softly.assertThat(key.type.toString()).isEqualTo("AVRO")
                    softly.assertThat(value.type.toString()).isEqualTo("AVRO")
                    val key = key.data as Map<String, Any>
                    val value = value.data as Map<String, Any>

                    softly.assertThat(key["id"])
                        .isEqualTo(id.id.toString())
                        .withFailMessage("id")
                    softly.assertThat(value["aNull"])
                        .isEqualTo(null)
                        .withFailMessage("aNull")
                    softly.assertThat(value["aBoolean"])
                        .isEqualTo(uberAvro.aBoolean)
                        .withFailMessage("aBoolean")
                    softly.assertThat(value["aInt"])
                        .isEqualTo(uberAvro.aInt)
                        .withFailMessage("aInt")
                    softly.assertThat(value["aLong"])
                        .isEqualTo(uberAvro.aLong)
                        .withFailMessage("aLong")
                    softly.assertThat(value["aFloat"].toString().substring(0, 6))
                        .isEqualTo(uberAvro.aFloat.toString().substring(0, 6))
                        .withFailMessage("aFloat")
                    softly.assertThat(value["aDouble"])
                        .isEqualTo(uberAvro.aDouble)
                        .withFailMessage("aDouble")
                    softly.assertThat(value["aEnum"])
                        .isEqualTo(uberAvro.aEnum.toString())
                        .withFailMessage("aEnum")
                    softly.assertThat(value["aStringArray"])
                        .isEqualTo(uberAvro.aStringArray.map { it.toString() })
                        .withFailMessage("aStringArray")
                    softly.assertThat(value["aLongMap"])
                        .isEqualTo(uberAvro.aLongMap)
                        .withFailMessage("aLongMap")
                    softly.assertThat(value["aFixed"])
                        .isNotNull
                        .withFailMessage("aFixed")
                    softly.assertThat(value["aString"])
                        .isEqualTo(uberAvro.aString.toString())
                        .withFailMessage("aString")
                    softly.assertThat((value["aRecord"] as Map<String, Any>)["nestedField"].toString())
                        .isEqualTo(uberAvro.aRecord.nestedField.toString())
                        .withFailMessage("aRecord.nestedField")
                }
            }
    }

    @Topic("consumer-test-6")
    @Test
    fun `should start streaming Kafka events with key Id and value UberAvro`(
        @ProducerOptions(
            keySerializer = KafkaAvroSerializer::class,
            valueSerializer = KafkaAvroSerializer::class
        )
        producer: Producer<SpecificRecord, SpecificRecord>
    ) {
        val stream =
            SseClient.create(URL("http://localhost:8080"))
                .eventStream("/api/events?t0=consumer-test-6", Argument.of(KafkaEvent::class.java))

        var receivedRecord: KafkaEvent? = null
        Flux.from(stream)
            .subscribe { event ->
                receivedRecord = event.data
            }

        val id = Id(UUID.randomUUID().toString())
        val address = randomAddress(id.id)
        producer.send(ProducerRecord("consumer-test-6", id, address)).get()

        await()
            .atMost(Duration.ofSeconds(10))
            .until { receivedRecord != null }

        assertSoftly { softly ->
            val key = receivedRecord!!.key.data as Map<String, Any?>
            val value = receivedRecord!!.value.data as Map<String, Any?>
            softly.assertThat(key["id"]).isEqualTo(id.id)
            softly.assertThat(value["id"]).isEqualTo(address.id)
            softly.assertThat(value["address"]).isEqualTo(address.address)
            softly.assertThat(value["personId"]).isEqualTo(address.personId)
        }
    }
}

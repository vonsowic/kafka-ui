package io.vonsowic.test

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.annotation.Client
import io.vonsowic.*
import net.datafaker.Faker
import org.apache.avro.generic.GenericData
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

@IntegrationTest
class ProducerTest(
    @Client("/")
    private val httpClient: AppClient
) {

    @Test
    fun `should send Kafka event with key string and value string`(
        @ConsumerOptions(
            topic = "test"
        ) consumer: TestConsumer<String, String>
    ) {
        val req =
            KafkaEventCreateReq(
                topic = "test",
                event = KafkaEvent(
                    key = KafkaEventPart(
                        data = UUID.randomUUID().toString(),
                        type = KafkaEventPartType.STRING
                    ),
                    value = KafkaEventPart(
                        data = UUID.randomUUID().toString(),
                        type = KafkaEventPartType.STRING
                    )
                )
            )

        httpClient.sendEvent(req)
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
            }

        val record = consumer.poll()
        assertThat(record?.key()).isEqualTo(req.event!!.key.data)
        assertThat(record?.key()).isEqualTo(req.event!!.key.data)
        assertThat(consumer.poll()).isNull()
    }

    @Test
    fun `should send Kafka event with null key and null value`(
        @ConsumerOptions(
            topic = "test"
        ) consumer: TestConsumer<String, String>
    ) {
        val req =
            KafkaEventCreateReq(
                topic = "test",
                event = KafkaEvent(
                    key = KafkaEventPart.NIL,
                    value = KafkaEventPart.NIL
                )
            )

        httpClient.sendEvent(req)
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
            }

        val record = consumer.poll()
        assertThat(record?.key()).isNull()
        assertThat(record?.value()).isNull()
        assertThat(consumer.poll()).isNull()
    }

    @Test
    fun `should send Kafka event with string key and Avro value`(
        @ConsumerOptions(
            topic = "test",
            valueDeserializer = KafkaAvroDeserializer::class,
        ) consumer: TestConsumer<String, GenericData.Record>
    ) {
        val personId = UUID.randomUUID().toString()
        val person =
            mapOf<String, Any>(
                "id" to personId,
                "firstName" to Faker.instance().name().firstName(),
                "lastName" to Faker.instance().name().lastName(),
                "birthDate" to Faker.instance().date().birthday().toInstant().toEpochMilli()
            )

        CachedSchemaRegistryClient("http://localhost:8081", 1)
            .register("test-value", AvroSchema(PeopleSchema.toString()))

        val req =
            KafkaEventCreateReq(
                topic = "test",
                event = KafkaEvent(
                    key = KafkaEventPart(
                        data = personId,
                        type = KafkaEventPartType.STRING
                    ),
                    value = KafkaEventPart(
                        data = person,
                        type = KafkaEventPartType.AVRO
                    ),
                )
            )

        httpClient.sendEvent(req)
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
            }

        val record = consumer.poll()
        assertThat(record?.key()).isEqualTo(personId)
        with(record!!.value()) {
           assertThat(get("id").toString()).isEqualTo(person["id"])
           assertThat(get("firstName").toString()).isEqualTo(person["firstName"])
           assertThat(get("lastName").toString()).isEqualTo(person["lastName"])
           assertThat(get("birthDate")).isEqualTo(person["birthDate"])
        }
        assertThat(consumer.poll()).isNull()
    }
}

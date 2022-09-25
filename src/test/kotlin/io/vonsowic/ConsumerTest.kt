package io.vonsowic

import io.micronaut.http.HttpStatus
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.*

const val EMPTY_TOPIC = "empty-topic"
const val TEST_TOPIC = "test-topic"

@Topic(topic = TEST_TOPIC)
@IntegrationTest
class ConsumerTest(
    private val httpClient: AppClient
) {

    @Topic(topic = EMPTY_TOPIC)
    @Test
    fun `should fetch empty list`() {
        with(httpClient.fetchEvents(EMPTY_TOPIC)) {
            assertThat(status).isEqualTo(HttpStatus.OK)
            assertThat(body()).isEmpty()
        }
    }

    @Test
    fun `should fetch all Kafka events with key string and value string`(
        @ProducerOptions
        producer: Producer<String, String>
    ) {
        val testKey = "key-${UUID.randomUUID()}"
        val testValue = "value-${UUID.randomUUID()}"
        producer.send(ProducerRecord(TEST_TOPIC, testKey, testValue)).get()

        httpClient.fetchEvents(TEST_TOPIC)
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
}

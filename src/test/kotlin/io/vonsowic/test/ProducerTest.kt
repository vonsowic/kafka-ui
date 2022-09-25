package io.vonsowic.test

import io.micronaut.http.HttpStatus
import io.micronaut.http.client.annotation.Client
import io.vonsowic.*
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
}

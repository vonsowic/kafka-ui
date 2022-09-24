package io.vonsowic

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.util.UUID

@IntegrationTest
class ProducerTest(
    @Client("/")
    private val httpClient: HttpClient
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

        Mono.from(httpClient.exchange(HttpRequest.POST("/api/events", req)))
            .block()
            .apply {
                assertThat(status).isEqualTo(HttpStatus.OK)
            }

        val record = consumer.poll()
        assertThat(record?.key()).isEqualTo(req.event!!.key.data)
        assertThat(record?.key()).isEqualTo(req.event!!.key.data)
        assertThat(consumer.poll()).isNull()
    }
}

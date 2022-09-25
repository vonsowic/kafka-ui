package io.vonsowic

import io.micronaut.http.HttpStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test


@IntegrationTest
class MetadataTest(
    private val httpClient: AppClient
) {

    @Test
    fun `should return empty list`() {
        with(httpClient.listTopics()) {
            assertThat(status).isEqualTo(HttpStatus.OK)
            assertThat(body()).isEmpty()
        }
    }

    @Topic(topic = "topic-1")
    @Topic(topic = "topic-2")
    @Topic(topic = "topic-3")
    @Test
    fun `should return 3 topics`() {
        with(httpClient.listTopics()) {
            assertThat(status).isEqualTo(HttpStatus.OK)
            assertThat(body()).hasSize(3)
        }
    }
}
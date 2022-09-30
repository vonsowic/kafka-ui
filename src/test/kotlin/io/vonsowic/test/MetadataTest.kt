package io.vonsowic.test

import io.micronaut.http.HttpStatus
import io.vonsowic.AppClient
import io.vonsowic.IntegrationTest
import io.vonsowic.Topic
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

    @Topic(topic = "topic-1", partitions = 3)
    @Test
    fun `should describe topic`() {
        with(httpClient.describeTopic("topic-1")) {
            assertThat(status).isEqualTo(HttpStatus.OK)
            val topicDescription = body()
            assertThat(topicDescription.name).isEqualTo("topic-1")
            assertThat(topicDescription.partitions).hasSize(3)
        }
    }
}
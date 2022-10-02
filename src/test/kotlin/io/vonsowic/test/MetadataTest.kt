package io.vonsowic.test

import io.micronaut.http.HttpStatus
import io.vonsowic.AppClient
import io.vonsowic.IntegrationTest
import io.vonsowic.Topic
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Condition
import org.junit.jupiter.api.Test


@IntegrationTest
class MetadataTest(
    private val httpClient: AppClient
) {

    @Topic(topic = "metadata-test-1-1")
    @Topic(topic = "metadata-test-1-2")
    @Topic(topic = "metadata-test-1-3")
    @Test
    fun `should return 3 topics`() {
        with(httpClient.listTopics()) {
            assertThat(status).isEqualTo(HttpStatus.OK)
            assertThat(body())
                .has(Condition(
                    { topics -> topics.find { it.name == "metadata-test-1-1" } != null },
                    "metadata-test-1-1"
                ))
                .has(Condition(
                    { topics -> topics.find { it.name == "metadata-test-1-2" } != null },
                    "metadata-test-1-2"
                ))
                .has(Condition(
                    { topics -> topics.find { it.name == "metadata-test-1-3" } != null },
                    "metadata-test-1-3"
                ))
        }
    }

    @Topic(topic = "metadata-test-2", partitions = 3)
    @Test
    fun `should describe topic`() {
        with(httpClient.describeTopic("metadata-test-2")) {
            assertThat(status).isEqualTo(HttpStatus.OK)
            val topicDescription = body()
            assertThat(topicDescription.name).isEqualTo("metadata-test-2")
            assertThat(topicDescription.partitions).hasSize(3)
        }
    }
}
package io.vonsowic.services

import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.TopicPartitionInfo
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.UUID


data class PartitionMetadata(
    val id: Int
)

data class TopicMetadata(
    val name: String,
    val partitions: Collection<PartitionMetadata>
)

@Singleton
class MetadataService(
    private val admin: Admin
) {

    fun topicsMetadata(topics: Collection<String>): Flux<TopicMetadata> =
        admin.describeTopics(topics)
            .allTopicNames()
            .let { Mono.fromCallable { it.get() } }
            .flatMapIterable { it.values }
            .map { topicDescriptor ->
                with(topicDescriptor!!) {
                    TopicMetadata(
                        name = name(),
                        partitions = partitions().map(::toMetadata)
                    )
                }
            }

    private fun toMetadata(partition: TopicPartitionInfo): PartitionMetadata =
        PartitionMetadata(
            id = partition.partition()
        )
}
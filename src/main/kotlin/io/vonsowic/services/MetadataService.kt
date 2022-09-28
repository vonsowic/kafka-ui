package io.vonsowic.services

import io.vonsowic.ListTopicItem
import io.vonsowic.PartitionMetadata
import io.vonsowic.TopicMetadata
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.TopicPartitionInfo
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.UUID


@Singleton
class MetadataService(
    private val admin: Admin
) {

    private fun exists(topicName: String): Mono<Boolean> =
        listTopics()
            .count()
            .map { it > 0 }

    fun listTopics(): Flux<ListTopicItem> =
        admin
            .listTopics()
            .let { Mono.fromCallable { it.listings().get() } }
            .flatMapIterable { it }
            .map {
                ListTopicItem(
                    name = it.name()
                )
            }

    fun topicMetadata(topic: String): Mono<TopicMetadata> = topicsMetadata(listOf(topic)).single()

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
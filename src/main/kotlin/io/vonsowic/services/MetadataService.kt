package io.vonsowic.services

import io.vonsowic.ListTopicItem
import io.vonsowic.PartitionMetadata
import io.vonsowic.TopicMetadata
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition


@Singleton
class MetadataService(
    private val admin: Admin
) {

    private fun exists(topicName: String): Boolean =
        listTopics().find { it.name === topicName } != null

    fun listTopics(): Collection<ListTopicItem> =
        admin
            .listTopics()
            .listings()
            .get()
            .map { topicListing ->
                ListTopicItem(name = topicListing.name())
            }

    fun topicMetadata(topic: String): TopicMetadata? = topicsMetadata(listOf(topic)).firstOrNull()

    fun topicsMetadata(topics: Collection<String>): List<TopicMetadata> =
        admin.describeTopics(topics)
            .allTopicNames()
            .get()
            .let { topicToDescMap ->
                val topicsPartitions =
                    topicToDescMap
                        .values
                        .flatMap { topicDesc ->
                            topicDesc.partitions().map { TopicPartition(topicDesc.name(), it.partition()) }
                        }

                val earliestOffsets =
                    admin.listOffsets(topicsPartitions.associateWith { OffsetSpec.earliest() })
                        .all()
                        .get()

                val latestOffsets =
                    admin.listOffsets(topicsPartitions.associateWith { OffsetSpec.latest() })
                        .all()
                        .get()

                topicToDescMap.keys.map { topicName ->
                    TopicMetadata(
                        name = topicName,
                        partitions = topicToDescMap[topicName]!!.partitions()
                            .map { topicPartitionInfo ->
                                val partition = topicPartitionInfo.partition()
                                PartitionMetadata(
                                    id = partition,
                                    earliestOffset = earliestOffsets[TopicPartition(topicName, partition)]!!.offset(),
                                    latestOffset = latestOffsets[TopicPartition(topicName, partition)]!!.offset()
                                )
                            }
                    )
                }
            }
}
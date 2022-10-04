package io.vonsowic.services

import io.micronaut.context.BeanProvider
import io.vonsowic.utils.AppConsumer
import io.vonsowic.utils.AppConsumerOptions
import jakarta.inject.Singleton
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver

const val EARLIEST_OFFSET: Long = -1
const val LATEST_OFFSET: Long = Long.MAX_VALUE

@Singleton
class AppConsumersPool(
    private val optionsProvider: BeanProvider<AppConsumerOptions>,
    private val metadataService: MetadataService
) {

    fun consumer(topics: Collection<PollOption>): AppConsumer {
        val rangeByTopic = topics.associateBy { it.topicName }
        return metadataService
            .topicsMetadata(topics.map { it.topicName })
            .flatMap { topicMetadata ->
                topicMetadata
                    .partitions
                    .map { partition ->
                        TopicPartition(topicMetadata.name, partition.id)
                    }
            }
            .let { topicPartitions ->
                KafkaReceiver.create(
                    optionsProvider.get()
                        .assignment(topicPartitions)
                        .addAssignListener { assignedTopicPartitions ->
                            assignedTopicPartitions.forEach { assignedPartition ->
                                rangeByTopic[assignedPartition.topicPartition().topic()]
                                    ?.partitionRange
                                    ?.get(assignedPartition.topicPartition().partition())
                                    ?.startOffset
                                    ?.takeIf { it >= 0 }
                                    ?.let { offset -> assignedPartition.seek(offset) }
                            }
                        }
                )
            }
    }

}
package io.vonsowic.services

import io.micronaut.context.BeanProvider
import io.vonsowic.utils.AppConsumer
import io.vonsowic.utils.AppConsumerOptions
import jakarta.inject.Singleton
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver

@Singleton
class AppConsumersPool(
    private val optionsProvider: BeanProvider<AppConsumerOptions>,
) {

    fun consumer(topics: PollOptions): AppConsumer {
        val rangeByTopic = topics.topicOptions.associateBy { it.topicName }
        return KafkaReceiver.create(
            optionsProvider.get()
                .assignment(topics.topicPartitions())
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
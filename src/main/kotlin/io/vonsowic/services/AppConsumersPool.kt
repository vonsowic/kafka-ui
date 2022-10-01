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
    private val metadataService: MetadataService
) {

    fun consumer(topics: Collection<PollOption>): AppConsumer =
        metadataService
            .topicsMetadata(topics.map { it.topicName })
            .flatMap { topicMetadata ->
                topicMetadata
                    .partitions
                    .map { partition ->
                        TopicPartition(topicMetadata.name, partition.id)
                    }
            }
            .let { topicPartitions ->
                KafkaReceiver.create(optionsProvider.get().assignment(topicPartitions))
            }
}
package io.vonsowic.services

import io.micronaut.context.BeanProvider
import io.vonsowic.KafkaEventPart
import io.vonsowic.utils.AppConsumer
import io.vonsowic.utils.AppConsumerOptions
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import reactor.kafka.receiver.KafkaReceiver

@Singleton
class AppConsumersPool(
    private val optionsProvider: BeanProvider<AppConsumerOptions>,
    private val metadataService: MetadataService
) {

    fun consumer(topics: Collection<PollOption>): Mono<AppConsumer> =
        metadataService
            .topicsMetadata(topics.map { it.topicName })
            .concatMapIterable { topicMetadata ->
                topicMetadata
                    .partitions
                    .map { partition ->
                        TopicPartition(topicMetadata.name, partition.id)
                    }
            }
            .collectList()
            .map { topicPartitions ->
                KafkaReceiver.create(optionsProvider.get().assignment(topicPartitions))
            }
}
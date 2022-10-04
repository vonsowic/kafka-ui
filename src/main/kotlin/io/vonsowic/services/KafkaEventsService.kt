package io.vonsowic.services

import io.vonsowic.KafkaEventPart
import io.vonsowic.utils.AppEvent
import io.vonsowic.utils.topicPartition
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Flux
import reactor.kafka.receiver.ReceiverRecord


data class PollOptions(
    val topicOptions: Collection<PollOption>,
) {
    fun endOffset(topicPartition: TopicPartition): Long? = endOffset(topicPartition.topic(), topicPartition.partition())

    fun endOffset(topic: String, partition: Int): Long? =
        topicOptions
            .find { it.topicName == topic }
            ?.partitionRange
            ?.get(partition)
            ?.endOffset

    fun topicPartitions(): Set<TopicPartition> =
        topicOptions
            .flatMap { option ->
                option.partitionRange
                    .keys
                    .map { partition -> TopicPartition(option.topicName, partition) }
            }
            .toSet()
}

data class PollOption(
    val topicName: String,
    val partitionRange: Map<Int, PartitionRange> = mapOf()
)

data class PartitionRange(
    val startOffset: Long? = null,
    val endOffset: Long? = null,
)

@Singleton
class KafkaEventsService(private val consumersPool: AppConsumersPool) {

    fun poll(options: PollOptions): Flux<AppEvent> {
        if (options.topicPartitions().isEmpty()) {
            return Flux.empty()
        }

        val receiver = consumersPool.consumer(options)
        val currentOffsets = mutableMapOf<TopicPartition, Long>()
        return receiver
            .receive()
            .doOnNext { currentOffsets[it.topicPartition()] = it.offset() }
            .takeUntil {
                options.topicPartitions()
                    .all { topicPartition ->
                        options.endOffset(topicPartition)
                        ?.let { endOffset -> (currentOffsets[topicPartition] ?: Long.MIN_VALUE) >= (endOffset - 1) }
                        // do not close the stream if end offset is not specified
                        ?: false
                    }
            }
            .filter { record ->
                options.endOffset(record.topic(), record.partition())
                    ?.let { record.offset() < it }
                    ?: true
            }
            .map {
                ReceiverRecord(
                    with(it) {
                        ConsumerRecord(
                            topic(),
                            partition(),
                            offset(),
                            timestamp(),
                            timestampType(),
                            0L,
                            serializedKeySize(),
                            serializedValueSize(),
                            key() ?: KafkaEventPart.NIL,
                            value() ?: KafkaEventPart.NIL,
                            headers()
                        )
                    },
                    it.receiverOffset()
                )
            }
    }
}


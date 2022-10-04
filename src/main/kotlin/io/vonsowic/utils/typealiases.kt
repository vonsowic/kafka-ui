package io.vonsowic.utils

import io.vonsowic.KafkaEventPart
import org.apache.kafka.common.TopicPartition
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender

typealias AppConsumerOptions = ReceiverOptions<KafkaEventPart?, KafkaEventPart?>
typealias AppConsumer = KafkaReceiver<KafkaEventPart?, KafkaEventPart?>
typealias AppProducer = KafkaSender<KafkaEventPart?, KafkaEventPart?>
typealias AppEvent = ReceiverRecord<KafkaEventPart, KafkaEventPart>

fun ReceiverRecord<*, *>.topicPartition() = TopicPartition(topic(), partition())
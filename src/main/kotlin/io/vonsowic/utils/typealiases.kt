package io.vonsowic.utils

import io.vonsowic.KafkaEventPart
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender

typealias AppConsumerOptions = ReceiverOptions<KafkaEventPart, KafkaEventPart>
typealias AppConsumer = KafkaReceiver<KafkaEventPart, KafkaEventPart>
typealias AppProducer = KafkaSender<KafkaEventPart, KafkaEventPart>
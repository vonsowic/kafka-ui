package io.vonsowic.config

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.vonsowic.KafkaEventPart
import io.vonsowic.utils.DelegatingDeserializer
import io.vonsowic.utils.DelegatingSerializer
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import java.util.Properties


@Suppress("unused")
@Factory
class KafkaFactory {

    @Singleton
    fun kafkaConsumer(@Property(name = "kafka") config: Properties): KafkaReceiver<KafkaEventPart, KafkaEventPart> {
        val consumerProps = Properties()
        consumerProps.putAll(config)
        consumerProps[ConsumerConfig.CLIENT_ID_CONFIG] = "kafka-ui-consumer"
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "kafka-ui"
        consumerProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = DelegatingDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = DelegatingDeserializer::class.java

        val options = ReceiverOptions.create<KafkaEventPart, KafkaEventPart>(consumerProps)
        return KafkaReceiver.create(options.subscription(listOf("test-topic")))
    }

    @Singleton
    fun kafkaProducer(@Property(name = "kafka") config: Properties): KafkaSender<KafkaEventPart, KafkaEventPart> {
        val producerProps = Properties()
        producerProps.putAll(config)
        producerProps[ProducerConfig.CLIENT_ID_CONFIG] = "kafka-ui-main"
        producerProps[ProducerConfig.ACKS_CONFIG] = "all"
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = DelegatingSerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = DelegatingSerializer::class.java

        val senderOptions: SenderOptions<KafkaEventPart, KafkaEventPart> = SenderOptions.create(producerProps)
        return KafkaSender.create(senderOptions)
    }
}
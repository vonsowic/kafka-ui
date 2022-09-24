package io.vonsowic.config

import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.PropertySource
import io.vonsowic.KafkaEventPart
import io.vonsowic.utils.DelegatingDeserializer
import io.vonsowic.utils.DelegatingSerializer
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
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
    fun kafkaProducer(): KafkaSender<KafkaEventPart, KafkaEventPart> {
        val props: Map<String, Any> = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ProducerConfig.CLIENT_ID_CONFIG to "kafka-ui-main",
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to DelegatingSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to DelegatingSerializer::class.java,
        )

        val senderOptions: SenderOptions<KafkaEventPart, KafkaEventPart> = SenderOptions.create(props)
        return KafkaSender.create(senderOptions)
    }
}
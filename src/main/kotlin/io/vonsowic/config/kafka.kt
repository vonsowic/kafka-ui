package io.vonsowic.config

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.vonsowic.KafkaEventPart
import io.vonsowic.utils.AppConsumerOptions
import io.vonsowic.utils.AppProducer
import io.vonsowic.utils.DelegatingDeserializer
import io.vonsowic.utils.DelegatingSerializer
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import java.util.*


@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.VALUE_PARAMETER)
@Property(name = "kafka")
annotation class KafkaProperties

@Suppress("unused")
@Factory
class KafkaFactory {

    @Bean
    fun consumerOptions(@KafkaProperties config: Properties): AppConsumerOptions {
        val consumerProps = Properties()
        consumerProps.putAll(config)
        consumerProps[ConsumerConfig.CLIENT_ID_CONFIG] = "kafka-ui-${UUID.randomUUID()}"
        consumerProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        consumerProps[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = false
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = DelegatingDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = DelegatingDeserializer::class.java
        return ReceiverOptions.create(consumerProps)
    }

    @Singleton
    fun kafkaProducer(@KafkaProperties config: Properties): AppProducer {
        val producerProps = Properties()
        producerProps.putAll(config)
        producerProps[ProducerConfig.CLIENT_ID_CONFIG] = "kafka-ui-main"
        producerProps[ProducerConfig.ACKS_CONFIG] = "all"
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = DelegatingSerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = DelegatingSerializer::class.java

        val senderOptions: SenderOptions<KafkaEventPart, KafkaEventPart> = SenderOptions.create(producerProps)
        return KafkaSender.create(senderOptions)
    }

    @Singleton
    fun admin(@KafkaProperties config: Properties): Admin {
        return Admin.create(config)
    }

    @Requires(configuration = AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG)
    @Singleton
    fun schemaRegistryClient(@KafkaProperties config: Properties): SchemaRegistryClient {
        return config.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REFLECTION_CONFIG)
            .takeIf { !it.startsWith("mock") }
            ?.let { CachedSchemaRegistryClient(it, 500) }
            ?: MockSchemaRegistryClient()
    }
}
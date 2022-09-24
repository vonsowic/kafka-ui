package io.vonsowic

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.reflect.KClass

private const val TEST_CONSUMER_QUEUE_CAPACITY = 1000
private val TEST_CONSUMER_POLL_DURATION = Duration.ofMillis(250)

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
@ExtendWith(KafkaExtension::class)
@MicronautTest
annotation class IntegrationTest

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.VALUE_PARAMETER)
annotation class ConsumerOptions(
    val topic: String,
    val keyDeserializer: KClass<*> = StringDeserializer::class,
    val valueDeserializer: KClass<*> = StringDeserializer::class
)


class TestConsumer<K, V>(private val consumer: Consumer<K, V>) {

    private val queue: ArrayBlockingQueue<ConsumerRecord<K, V>> = ArrayBlockingQueue(TEST_CONSUMER_QUEUE_CAPACITY)

    init {
        thread {
            while (true) {
                consumer
                    .poll(TEST_CONSUMER_POLL_DURATION)
                    .let {
                        records ->
                        queue.addAll(records)
                    }
            }
        }
    }

    fun poll(duration: Duration = Duration.ofSeconds(3)): ConsumerRecord<K, V>? =
        queue.poll(duration.toMillis(), TimeUnit.MILLISECONDS)
}

class KafkaExtension : BeforeAllCallback, ParameterResolver {

    private val consumersByTopic = mutableMapOf<String, TestConsumer<*, *>>()
    private val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))

    override fun beforeAll(context: ExtensionContext?) {
        kafkaContainer.start()
        System.setProperty("kafka.bootstrap.servers", kafkaContainer.bootstrapServers)
    }

    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean =
        parameterContext
            .parameter
            .let { it.isAnnotationPresent(ConsumerOptions::class.java) }

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Any {
        return when (parameterContext.parameter.type) {
            TestConsumer::class.java -> consumer(parameterContext)
            else -> throw Exception("invalid parameter type")
        }
    }

    private fun consumer(parameterContext: ParameterContext): TestConsumer<*, *> =
        parameterContext
            .parameter
            .getAnnotation(ConsumerOptions::class.java)
            .let { options ->
                consumersByTopic.computeIfAbsent(options.topic) { createConsumer(options) }
            }

    private fun createConsumer(config: ConsumerOptions): TestConsumer<*, *> =
        Properties()
            .apply {
                this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
                this[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
                this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
                this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = config.keyDeserializer.java
                this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = config.valueDeserializer.java
            }
            .let { props -> KafkaConsumer<Any?, Any?>(props) }
            .apply { subscribe(listOf(config.topic)) }
            .let { kafkaConsumer -> TestConsumer(consumer = kafkaConsumer) }
}


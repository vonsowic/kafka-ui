package io.vonsowic

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.extension.*
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.reflect.KClass

private const val TEST_CONSUMER_QUEUE_CAPACITY = 1000
private val TEST_CONSUMER_POLL_DURATION = Duration.ofMillis(250)
private const val REPLICATION_FACTOR: Short = 1

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
@ExtendWith(KafkaExtension::class)
@MicronautTest(transactional = false)
annotation class IntegrationTest

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.VALUE_PARAMETER)
annotation class ConsumerOptions(
    val topic: String,
    val keyDeserializer: KClass<*> = StringDeserializer::class,
    val valueDeserializer: KClass<*> = StringDeserializer::class
)

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.VALUE_PARAMETER)
annotation class ProducerOptions(
    val keySerializer: KClass<*> = StringSerializer::class,
    val valueSerializer: KClass<*> = StringSerializer::class
)

@Repeatable
@Retention(AnnotationRetention.RUNTIME)
@Target(allowedTargets = [AnnotationTarget.CLASS, AnnotationTarget.FUNCTION])
annotation class Topic(
    val topic: String,
    val partitions: Int = 5
)


class TestConsumer<K, V>(private val consumer: Consumer<K, V>) {

    private val queue: ArrayBlockingQueue<ConsumerRecord<K, V>> = ArrayBlockingQueue(TEST_CONSUMER_QUEUE_CAPACITY)
    private val thread =
        thread {
            try {
                while (true) {
                    consumer
                        .poll(TEST_CONSUMER_POLL_DURATION)
                        .let { records ->
                            queue.addAll(records)
                        }
                }
            } catch (_: InterruptedException) {
                consumer.close()
            }
        }

    fun poll(duration: Duration = Duration.ofSeconds(3)): ConsumerRecord<K, V>? =
        queue.poll(duration.toMillis(), TimeUnit.MILLISECONDS)

    fun close() {
        thread.interrupt()
    }
}

class KafkaExtension : BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {
    companion object {
        private val consumersByTopic = mutableMapOf<String, TestConsumer<*, *>>()
        private lateinit var admin: Admin
        private var isRunning = false
        private val infra =
            DockerComposeContainer(File("docker-compose.yml"))
                .withExposedService("broker", 9092, Wait.forListeningPort())
                .withExposedService(
                    "schema-registry", 8081,
                    Wait.forHttp("/subjects")
                        .forStatusCode(200)
                )

        fun bootstrapServers(): String = "PLAINTEXT://0.0.0.0:9092"

        fun schemaRegistryUrl(): String = "http://localhost:8081"
    }

    override fun beforeAll(context: ExtensionContext) {
        if (isRunning) {
            return
        }

        infra.start()
        isRunning = true
        System.setProperty("kafka.${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}", bootstrapServers())
        System.setProperty("kafka.${AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG}", schemaRegistryUrl())

        admin =
            AdminClient.create(
                Properties()
                    .apply {
                        setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
                    }
            )
    }

    override fun beforeEach(context: ExtensionContext) {
        val classAnnotation =
            context
                .testClass.orElse(null)
                .let { it?.getDeclaredAnnotationsByType(Topic::class.java) ?: emptyArray() }

        val methodAnnotation =
            context
                .testMethod.orElse(null)
                .let { it?.getDeclaredAnnotationsByType(Topic::class.java) ?: emptyArray() }

        admin.create(classAnnotation.toList() + methodAnnotation.toList())
    }

    override fun afterEach(context: ExtensionContext) {
        consumersByTopic
            .values
            .forEach { it.close() }

        consumersByTopic.clear()
    }


    override fun supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean =
        parameterContext
            .parameter
            .let {
                it.isAnnotationPresent(ConsumerOptions::class.java) ||
                        it.isAnnotationPresent(ProducerOptions::class.java)
            }

    override fun resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Any {
        return when (parameterContext.parameter.type) {
            TestConsumer::class.java -> consumer(parameterContext)
            Producer::class.java -> producer(parameterContext)
            else -> throw Exception("invalid parameter type")
        }
    }

    private fun consumer(parameterContext: ParameterContext): TestConsumer<*, *> =
        parameterContext
            .parameter
            .getAnnotation(ConsumerOptions::class.java)
            .let { options ->
                consumersByTopic.computeIfAbsent(options.topic) {
                    createConsumer(options)
                }
            }

    private fun producer(parameterContext: ParameterContext): Producer<*, *> =
        parameterContext
            .parameter
            .getAnnotation(ProducerOptions::class.java)
            .let { options ->
                val props = Properties()
                props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers()
                props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = options.keySerializer.java
                props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = options.valueSerializer.java
                props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl()
                KafkaProducer<Any, Any>(props)
            }

    private fun createConsumer(config: ConsumerOptions): TestConsumer<*, *> =
        Properties()
            .apply {
                this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers()
                this[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
                this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
                this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = config.keyDeserializer.java
                this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = config.valueDeserializer.java
                this[KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl()
            }
            .let { props -> KafkaConsumer<Any?, Any?>(props) }
            .apply { subscribe(listOf(config.topic)) }
            .let { kafkaConsumer -> TestConsumer(consumer = kafkaConsumer) }

//    private fun findTopicAnnotations(annotatedClass: Class<*>): Array<out Topic> =
//        findAnnotations(annotatedClass, Topic::class.java)
//    private fun <A : Annotation> findAnnotations(annotatedClass: Class<*>, annotationType: Class<A>): Array<out A> {
//        return annotatedClass.getDeclaredAnnotationsByType(annotationType)
//            + annotatedClass.declaredAnnotations.flatMap { findAnnotations(it::class.java, annotationType) }
//    }
}

private fun Admin.create(topics: List<Topic>) =
    topics
        .map { NewTopic(it.topic, it.partitions, REPLICATION_FACTOR) }
        .let(this::createTopics)
        .all()
        .get()

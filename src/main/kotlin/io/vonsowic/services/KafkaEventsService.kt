package io.vonsowic.services

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.vonsowic.KafkaEventCreateReq
import io.vonsowic.KafkaEventPart
import io.vonsowic.KafkaEventPartType
import io.vonsowic.utils.AppEvent
import io.vonsowic.utils.AppException
import io.vonsowic.utils.AppProducer
import io.vonsowic.utils.completeOnIdleStream
import jakarta.inject.Singleton
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.SenderRecord
import java.time.Duration
import java.util.*


data class PollOptions(
    val topicOptions: Collection<PollOption>,
    // if null, then stream is never closed on
    val maxIdleTime: Duration? = null
)

data class PollOption(
    val topicName: String,
)

@Singleton
class KafkaEventsService(
    private val appProducer: AppProducer,
    private val consumersPool: AppConsumersPool,
    private val schemaRegistryClient: Optional<SchemaRegistryClient>
) {

    fun send(request: KafkaEventCreateReq): Mono<Void> =
        request.toSenderRecord()
            .let { SenderRecord.create(it, null) }
            .let { Mono.just(it) }
            .let { appProducer.send(it) }
            .doOnNext { println("record has been published to topic ${request.topic}") }
            .then()

    fun poll(options: PollOptions): Flux<AppEvent> =
        consumersPool
            .consumer(options.topicOptions)
            .flatMapMany { it.receive() }
            .let {
                if (options.maxIdleTime != null) {
                    it.completeOnIdleStream(maxIdleTime = options.maxIdleTime)
                } else {
                    it
                }
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


    private fun KafkaEventCreateReq.toSenderRecord() =
        ProducerRecord(
            this.topic,
            this.partition,
            null,
            this.event?.key?.let { parse(this.topic, true, it) },
            this.event?.value?.let { parse(this.topic, false, it) },
            this.event?.headers?.map { (key, value) -> RecordHeader(key, value.toByteArray()) }
        )

    private fun parse(topicName: String, isKey: Boolean, eventPart: KafkaEventPart): KafkaEventPart =
        when (eventPart.type) {
            KafkaEventPartType.AVRO ->
                KafkaEventPart(
                    data = toAvro(
                        topicName,
                        isKey,
                        eventPart.data
                            ?: throw AppException("KafkaEventPart.data must not be null if KafkaEventPart.data is not NIL")
                    ),
                    type = eventPart.type
                )
            else -> eventPart
        }

    private fun toAvro(topicName: String, isKey: Boolean, data: Any): GenericData.Record {
        val client = schemaRegistryClient()
        val schema =
            client
                .getLatestSchemaMetadata(topicName.toSubject(isKey))
                .let { metadata -> client.getSchemaById(metadata.id) }
                .let { Schema.Parser().parse(it.canonicalString()) }

        val avro = data as Map<String, Any>
        return GenericRecordBuilder(schema)
            .apply {
                avro.entries
                    .forEach { (key, value) ->
                        set(key, value)
                    }
            }
            .build()
    }

    private fun schemaRegistryClient(): SchemaRegistryClient =
        schemaRegistryClient.orElseThrow { AppException("schema registry is not configured") }

    private fun String.toSubject(isKey: Boolean) =
        "${this}-${if (isKey) "key" else "value"}"
}


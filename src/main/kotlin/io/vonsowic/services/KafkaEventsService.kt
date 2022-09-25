package io.vonsowic.services

import io.vonsowic.KafkaEvent
import io.vonsowic.KafkaEventCreateReq
import io.vonsowic.utils.AppConsumer
import io.vonsowic.utils.AppProducer
import io.vonsowic.utils.completeOnIdleStream
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.sender.SenderRecord
import java.time.Duration


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
) {

    fun send(request: KafkaEventCreateReq): Mono<Void> =
        request
            .let(KafkaEventCreateReq::toSenderRecord)
            .let { SenderRecord.create(it, null) }
            .let { Mono.just(it) }
            .let { appProducer.send(it) }
            .doOnNext { println("record has been published to topic ${request.topic}") }
            .then()

    fun poll(options: PollOptions): Flux<KafkaEvent> =
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
            .map { record ->
                KafkaEvent(
                    key = record.key(),
                    value = record.value(),
                    headers = record.headers()
                        ?.filterNotNull()
                        ?.associate { header -> header.key() to (header.value()?.toString() ?: "") }
                        ?: mapOf()
                )
            }


}

private fun KafkaEventCreateReq.toSenderRecord() =
    ProducerRecord(
        this.topic,
        this.partition,
        null,
        this.event?.key,
        this.event?.value,
        this.event?.headers?.map { (key, value) -> RecordHeader(key, value.toByteArray()) }
    )

package io.vonsowic.services

import io.vonsowic.KafkaEvent
import io.vonsowic.KafkaEventCreateReq
import io.vonsowic.KafkaEventPart
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import java.time.Duration
import java.time.Instant.now

private val TICK_DURATION = Duration.ofMillis(100)

@Singleton
class KafkaEventsService(
    private val kafkaProducer: KafkaSender<KafkaEventPart, KafkaEventPart>,
    private val kafkaConsumer: KafkaReceiver<KafkaEventPart, KafkaEventPart>,
) {

    fun send(request: KafkaEventCreateReq): Mono<Void> =
        request
            .let(KafkaEventCreateReq::toSenderRecord)
            .let { SenderRecord.create(it, null) }
            .let { Mono.just(it) }
            .let { kafkaProducer.send(it) }
            .doOnNext { println("record has been published to topic ${request.topic}") }
            .then()

    fun poll(): Flux<KafkaEvent> =
        kafkaConsumer
            .receive()
            .completeOnIdleStream(maxIdleTime = Duration.ofSeconds(1))
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

private fun <E> Flux<E>.completeOnIdleStream(maxIdleTime: Duration): Flux<E> {
    var lastEvent = now()
    return Flux.create { sink ->
        val subscription =
            this.doOnNext { lastEvent = now() }
                .subscribe { sink.next(it) }

        Flux.interval(Duration.ZERO, TICK_DURATION)
            .filter { lastEvent + maxIdleTime < now() }
            .take(1)
            .subscribe {
                subscription.dispose()
                sink.complete()
            }
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

package io.vonsowic.controllers

import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.QueryValue
import io.vonsowic.KafkaEvent
import io.vonsowic.KafkaEventCreateReq
import io.vonsowic.KafkaEventPart
import io.vonsowic.KafkaEventPartType
import io.vonsowic.services.KafkaEventsService
import io.vonsowic.services.PollOption
import io.vonsowic.services.PollOptions
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

@Suppress("unused")
@Controller("/api/events")
class KafkaEventsController(
    private val kafkaEventsService: KafkaEventsService
) {

    @Post
    fun sendEvent(@Body req: KafkaEventCreateReq): Mono<Void> =
        kafkaEventsService.send(req)

    @Get
    fun poll(
        @QueryValue("topic") topics: List<String>
    ): Flux<KafkaEvent> =
        kafkaEventsService
            .poll(
                PollOptions(
                    topicOptions = topics.map { topic ->
                        PollOption(topicName = topic)
                    },
                    maxIdleTime = Duration.ofSeconds(1)
                )
            )
            .map {
                KafkaEvent(
                    key = it.key.toMapIfAvro(),
                    value = it.value.toMapIfAvro(),
                    headers = it.headers,
                )
            }
}

private fun KafkaEventPart.toMapIfAvro(): KafkaEventPart =
    if (this.type == KafkaEventPartType.AVRO) {
        val avroData = this.data as GenericData.Record
        KafkaEventPart(
            type = KafkaEventPartType.AVRO,
            data = avroData
                .schema
                .fields
                .associate { field -> field.name() to avroData.value(field) }
        )
    } else {
        this
    }

private fun GenericData.Record.value(field: Schema.Field): Any? =
    when (val value = this[field.name()]) {
        is Utf8 -> value.toString()
        else -> value
    }

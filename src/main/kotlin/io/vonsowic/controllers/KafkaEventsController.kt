package io.vonsowic.controllers

import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.vonsowic.KafkaEvent
import io.vonsowic.KafkaEventCreateReq
import io.vonsowic.services.KafkaEventsService
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Suppress("unused")
@Controller("/api/events")
class KafkaEventsController(
    private val kafkaEventsService: KafkaEventsService
) {

    @Post
    fun sendEvent(@Body req: KafkaEventCreateReq): Mono<Void> =
        kafkaEventsService.send(req)

    @Get
    fun poll(): Flux<KafkaEvent> =
        kafkaEventsService.poll()
}
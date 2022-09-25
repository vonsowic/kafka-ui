package io.vonsowic.controllers

import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.QueryValue
import io.vonsowic.KafkaEvent
import io.vonsowic.KafkaEventCreateReq
import io.vonsowic.services.KafkaEventsService
import io.vonsowic.services.PollOption
import io.vonsowic.services.PollOptions
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
        kafkaEventsService.poll(
            PollOptions(
                topicOptions = topics.map { topic ->
                    PollOption(topicName = topic)
                },
                maxIdleTime = Duration.ofSeconds(1)
            )
        )
}
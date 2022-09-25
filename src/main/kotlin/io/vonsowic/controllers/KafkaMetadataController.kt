package io.vonsowic.controllers

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.vonsowic.ListTopicItem
import io.vonsowic.services.MetadataService
import reactor.core.publisher.Flux

@Suppress("unused")
@Controller("/api/topics")
class KafkaMetadataController(
    private val metadataService: MetadataService
) {

    @Get
    fun listTopics(): Flux<ListTopicItem> =
        metadataService.listTopics()

}
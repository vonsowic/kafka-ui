package io.vonsowic.controllers

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.vonsowic.ListTopicItem
import io.vonsowic.TopicMetadata
import io.vonsowic.services.MetadataService
import reactor.core.publisher.Flux

@Suppress("unused")
@Controller("/api/topics")
class KafkaMetadataController(
    private val metadataService: MetadataService
) {

    @Get
    fun listTopics(): Collection<ListTopicItem> =
        metadataService.listTopics()

    @Get("/{topicName}")
    fun describe(@PathVariable topicName: String): TopicMetadata? =
        metadataService.topicsMetadata(listOf(topicName))
            .firstOrNull()
}
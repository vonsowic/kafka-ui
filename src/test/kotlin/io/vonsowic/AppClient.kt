package io.vonsowic

import io.micronaut.http.annotation.QueryValue
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post


@Client("/")
interface AppClient {

    @Post("/api/events")
    fun sendEvent(@Body body: KafkaEventCreateReq): HttpResponse<Void>

    @Get("/api/events")
    fun fetchEvents(@QueryValue("topic") topic: String): HttpResponse<List<KafkaEvent>>

    @Get("/api/events")
    fun fetchEvents(@QueryValue("topic") topics: Collection<String>): HttpResponse<List<KafkaEvent>>

    @Get("/api/topics")
    fun listTopics(): HttpResponse<List<ListTopicItem>>
}
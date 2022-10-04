package io.vonsowic

import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.QueryValue
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.exceptions.HttpClientResponseException
import org.assertj.core.api.Assertions.assertThat

class AppHttpClientConfig : DefaultHttpClientConfiguration() {
    init {
//        setConnectTimeout(Duration.ofSeconds(30))
//        setReadTimeout(Duration.ofSeconds(30))
    }
}

@Client(value = "/api", configuration = AppHttpClientConfig::class)
interface AppClient {

    @Post("/events")
    fun sendEvent(@Body body: KafkaEventCreateReq): HttpResponse<Void>

    @Get("/events")
    fun fetchEvents(@QueryValue("t0") topic: String): HttpResponse<List<KafkaEvent>>

    @Get("/topics")
    fun listTopics(): HttpResponse<List<ListTopicItem>>

    @Get("/topics/{topic}")
    fun describeTopic(@PathVariable topic: String): HttpResponse<TopicMetadata>


    @Post("/sql")
    fun sql(@Body req: SqlStatementReq): HttpResponse<List<SqlStatementRow>>
}

fun AppClient.expectError(call: AppClient.() -> HttpResponse<out Any>): HttpResponse<*> =
    try {
        val res = call()
        throw AppClientException(res.status)
    } catch (ex: HttpClientResponseException) {
        ex.response
    }

fun <R> AppClient.expectStatus(status: HttpStatus, call: AppClient.() -> HttpResponse<out Any>): HttpResponse<R> =
    try {
        val res = call()
        assertThat(res.status).isEqualTo(status)
        res
    } catch (ex: HttpClientResponseException) {
        assertThat(ex.status).isEqualTo(status)
        ex.response
    } as HttpResponse<R>

class AppClientException(status: HttpStatus) : Exception("Expected error response, got response with status $status")
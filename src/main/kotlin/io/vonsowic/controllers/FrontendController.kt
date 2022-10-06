package io.vonsowic.controllers

import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Produces
import java.nio.file.Files
import java.nio.file.Path


@Controller("/")
class FrontendController(
    @Value("\${kafka-ui.ui-dir}")
    private val indexHtmlFilePath: String
) {

    @Produces(MediaType.TEXT_HTML)
    @io.micronaut.http.annotation.Error(status = HttpStatus.NOT_FOUND, global = true)
    fun forwardToFrontend(request: HttpRequest<Any>): HttpResponse<out Any> {
        return HttpResponse.ok(Files.readString(Path.of(indexHtmlFilePath, "index.html")))
    }
}
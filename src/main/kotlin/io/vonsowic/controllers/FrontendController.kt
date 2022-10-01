package io.vonsowic.controllers

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import java.net.URI


@Controller("/")
class FrontendController {

    @io.micronaut.http.annotation.Error(status = HttpStatus.NOT_FOUND, global = true)
    fun forwardToFrontend(request: HttpRequest<Any>): HttpResponse<out Any> {
       return HttpResponse.redirect(URI("/"))
    }
}
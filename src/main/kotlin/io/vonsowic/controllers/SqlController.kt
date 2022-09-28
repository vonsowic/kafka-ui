package io.vonsowic.controllers

import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.vonsowic.SqlStatementReq
import io.vonsowic.SqlStatementRow
import io.vonsowic.services.SqlService
import reactor.core.publisher.Flux

@Suppress("unused")
@Controller("/api/sql")
class SqlController(
    private val sqlService: SqlService,
) {


    @Post(
        consumes = [MediaType.APPLICATION_JSON],
        produces = [MediaType.APPLICATION_JSON]
    )
    fun executeSqlStatement(@Body req: SqlStatementReq): Flux<SqlStatementRow> =
        sqlService.executeSqlStatement(req)
}
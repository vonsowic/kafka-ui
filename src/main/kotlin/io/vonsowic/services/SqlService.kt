package io.vonsowic.services

import io.r2dbc.pool.ConnectionPool
import io.vonsowic.SqlStatementReq
import io.vonsowic.SqlStatementRow
import jakarta.inject.Singleton
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Singleton
class SqlService(
    private val connectionPool: ConnectionPool
) {

    /**
     * first row contains column names
     * following rows contain values
     */
    fun executeSqlStatement(req: SqlStatementReq): Flux<SqlStatementRow> =
        Flux.fromIterable(extractTables(req))
            .flatMap(::createTableIfNotExists)
            .map { listOf<Any>() }


    private fun extractTables(req: SqlStatementReq): Collection<String> =
        listOf()

    private fun createTableIfNotExists(table: String): Mono<String> =
        tableExists(table)
            .flatMap { exists ->
                if (exists) {
                    Mono.just(table)
                } else {
                    createTable(table).thenReturn(table)
                }
            }

    private fun tableExists(table: String): Mono<Boolean> =
        connectionPool
            .create()
            .map { false }

    private fun createTable(table: String): Mono<Boolean> = Mono.just(true)
}
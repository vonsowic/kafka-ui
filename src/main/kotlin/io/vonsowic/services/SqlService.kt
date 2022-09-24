package io.vonsowic.services

import io.vonsowic.SqlStatementReq
import io.vonsowic.SqlStatementRow
import jakarta.inject.Singleton
import reactor.core.publisher.Flux

@Singleton
class SqlService {

    /**
     * first row contains column names
     * following rows contain values
     */
    fun executeSqlStatement(req: SqlStatementReq): Flux<SqlStatementRow> =
        Flux.just(
            listOf("col1", "col2"),
            listOf("val1", 123)
        )
}
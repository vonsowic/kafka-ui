package io.vonsowic.services

import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.micronaut.data.r2dbc.operations.R2dbcOperations
import io.r2dbc.spi.Result
import io.vonsowic.KafkaEventPart
import io.vonsowic.KafkaEventPartType.*
import io.vonsowic.SqlStatementReq
import io.vonsowic.SqlStatementRow
import io.vonsowic.utils.ClientException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import io.vonsowic.utils.AppEvent
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData


private val log = LoggerFactory.getLogger(SqlService::class.java)
private val WHITESPACE_REGEX = Regex("\\s")
private val KEYWORDS_BEFORE_TABLE_NAME = listOf("FROM", "JOIN")

@Singleton
class SqlService(
    private val metadataService: MetadataService,
    private val db: R2dbcOperations,
    private val schemaRegistryClient: SchemaRegistryClient,
    private val eventsService: KafkaEventsService
) {

    val mirroredTopics = mutableMapOf<String, Map<Long, MirroredPartition>>()

    data class MirroredPartition(
        val startingOffset: Long,
        var currentOffset: Long
    )

    /**
     * first row contains column names
     * following rows contain values
     */
    fun executeSqlStatement(req: SqlStatementReq): Flux<SqlStatementRow> =
        with(extractTables(req)) {
            init(this)
                .then(Mono.create<Void> {
                    // send complete signal when initial select is completed
                    while (true) {

                        it.success()
                        return@create
                    }
                })
                .thenMany(select(req))
        }


    private fun init(tables: Collection<String>): Mono<Void> =
        // verify if requested topics exist
        metadataService.listTopics()
            .map { it.name }
            .let { topics ->
                val notExistingTopics = tables.filter { !topics.contains(it) }
                if (notExistingTopics.isNotEmpty()) {
                    throw ClientException("Following topics does not exist: $notExistingTopics")
                }

                topics.forEach { topic ->
                    mirrorTopic(topic)
                }
            }
            .let { Mono.empty() }

    private fun mirrorTopic(topic: String) {
        createTable(topic)
            .doOnNext {
                log.info("Table for topic $topic has been created. Number of rows updated: $it")
            }
            .then(db.connectionFactory().create().let { Mono.from(it) })
            .flatMapMany { conn ->
                eventsService
                    .poll(
                        PollOptions(
                            topicOptions = listOf(
                                PollOption(
                                    topicName = topic
                                )
                            ),
                        )
                    )
                    .flatMap { event ->
                        conn.createStatement(toInsertStatement(event))
                            .execute()
                            .let { Mono.from(it) }
                            .thenReturn(event)
                    }
                    .doOnNext {
                        log.debug("inserted record from topic $topic, partition: ${it.partition()}, offset: ${it.offset()}")
                    }
            }
            .subscribe()

        log.info("Started streaming data from topic $topic to db")
    }

    private fun extractTables(req: SqlStatementReq): Collection<String> =
        with(req.sql.split(WHITESPACE_REGEX)) {
            withIndex()
                .filter { KEYWORDS_BEFORE_TABLE_NAME.contains(it.value.uppercase()) }
                .map { it.index + 1 }
                .map { get(it) }
        }

    private fun createTable(table: String): Mono<Void> =
        db.connectionFactory()
            .create()
            .let { Mono.from(it) }
            .flatMap { conn ->
                conn.createStatement(ddl(table))
                    .execute()
                    .let { Mono.from(it) }
            }
            .then()

    private fun ddl(table: String): String {
        val columns = mutableListOf<String>()
        columns += "__partition INTEGER"
        columns += "__offset BIGINT"

        schemaRegistryClient
            .runCatching { getLatestSchemaMetadata("$table-key") }
            .getOrNull()
            ?.let(::ddl)
            ?.run(columns::addAll)

        schemaRegistryClient
            .runCatching { getLatestSchemaMetadata("$table-value") }
            .getOrNull()
            ?.let(::ddl)
            ?.filter { !columns.contains(it) }
            ?.run(columns::addAll)

        return "CREATE TABLE $table ( ${columns.joinToString(separator = ",")} )"
    }

    private fun ddl(schemaMetadata: SchemaMetadata): Collection<String> =
        schemaMetadata
            .schema
            .let { Parser().parse(it) }
            .let { schema ->
                schema.fields.map { field ->
                    "${field.name()} ${avroToDbType(field)}"
                }
            }

    private fun avroToDbType(field: Schema.Field): String =
        when (val type = field.schema().type ?: throw Exception("Field schema type cannot be null")) {
            Schema.Type.RECORD -> TODO()
            Schema.Type.ENUM -> TODO()
            Schema.Type.ARRAY -> TODO()
            Schema.Type.MAP -> TODO()
            Schema.Type.UNION ->
                if (field.schema().isNullable) {
                    avroToDbType(field.schema().types.find { !it.isNullable }!!.type)
                } else {
                    TODO()
                }
            Schema.Type.FIXED -> TODO()
            Schema.Type.BYTES -> TODO()
            Schema.Type.FLOAT -> TODO()
            Schema.Type.DOUBLE -> TODO()
            Schema.Type.BOOLEAN -> TODO()
            Schema.Type.NULL -> TODO()
            else -> avroToDbType(type)
        }

    private fun avroToDbType(type: Schema.Type): String =
        when (type) {
            Schema.Type.STRING -> "VARCHAR(10000)"
            Schema.Type.INT -> "INTEGER"
            Schema.Type.LONG -> "BIGINT"
            else -> throw Exception("Unhandled type: $type")
        }

    private fun toInsertStatement(event: AppEvent): String {
        val valueColumns = eventColumns(event.value())
        val fields = "__partition, __offset, " + valueColumns.columns.joinToString(", ")
        val values = "${event.partition()}, ${event.offset()}, " + valueColumns.rows.joinToString(", ")
        return """
            INSERT INTO ${event.topic()} ($fields)
            VALUES ($values)
        """.trimIndent()
    }

    private fun eventColumns(event: KafkaEventPart): DbEvent =
        when (event.type) {
            STRING -> TODO()
            AVRO ->
                with(event.data as GenericData.Record) {
                    val columns = schema.fields.map { it.name() }
                    DbEvent(
                        columns = columns,
                        rows = columns.map { "'${get(it)}'" }
                    )
                }
            NIL -> DbEvent(columns = listOf(), rows = listOf())
        }

    private fun select(req: SqlStatementReq): Flux<SqlStatementRow> =
        db.connectionFactory().create().let { Mono.from(it) }
            .flatMap { conn ->
                conn.createStatement(req.sql)
                    .execute()
                    .let { Mono.from(it) }
            }
            .flatMapMany(::toRow)

    private fun toRow(result: Result): Flux<SqlStatementRow> {
        var isFirstRow = true
        return Flux.create { sink ->
            Flux.from(result.map { row, _ -> row })
                .doOnComplete { sink.complete() }
                .doOnError { sink.error(it) }
                .subscribe { row ->
                    val columns = row.metadata.columnMetadatas.map { it.name }
                    if (isFirstRow) {
                        isFirstRow = false
                        sink.next(columns)
                    }

                    sink.next(columns.map { row[it] })
                }
        }
    }

    data class DbEvent(
        val columns: List<String>,
        val rows: List<Any?>
    )
}

package io.vonsowic.services

import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.micronaut.data.r2dbc.operations.R2dbcOperations
import io.r2dbc.spi.Connection
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
import org.apache.avro.util.Utf8


private val log = LoggerFactory.getLogger(SqlService::class.java)
private val WHITESPACE_REGEX = Regex("\\s")
private val KEYWORDS_BEFORE_TABLE_NAME = listOf("FROM", "JOIN")

@Singleton
class SqlService(
    private val metadataService: MetadataService,
    db: R2dbcOperations,
    private val schemaRegistryClient: SchemaRegistryClient,
    private val eventsService: KafkaEventsService
) {

    private val mirroredTopics = mutableMapOf<String, Map<Int, MirroredPartition>>()

    data class MirroredPartition(
        val startingOffset: Long,
        var currentOffset: Long
    )

    private fun Map<Int, MirroredPartition>.isMirrored(): Boolean =
        values.all { mirroredPartition ->
            with(mirroredPartition) {
                currentOffset >= startingOffset
            }
        }

    private val connection: Mono<Connection> =
        db.connectionFactory()
            .create()
            .let { Mono.from(it) }

    /**
     * first row contains column names
     * following rows contain values
     */
    fun executeSqlStatement(req: SqlStatementReq): Flux<SqlStatementRow> =
        extractTables(req)
            .let { tables ->
                init(tables)
                    .then(Mono.create<Void> { sink ->
                        // wait until all tables are mirrored
                        // send complete signal when initial select is completed
                        while (true) {
                            val allRequestedTablesMirrored =
                                tables.map { mirroredTopics[it] }
                                    .all { it?.isMirrored() ?: false }

                            if (allRequestedTablesMirrored) {
                                sink.success()
                                return@create
                            }

                            Thread.sleep(20)
                        }
                    })
                    .thenMany(select(req))
            }


    private fun init(tables: Collection<String>): Mono<Void> =
        // verify if requested topics exist
        metadataService.topicsMetadata(tables)
            .let { topics ->
                val notExistingTopics =
                    tables.filter { table ->
                        topics.find { table == it.name } == null
                    }

                if (notExistingTopics.isNotEmpty()) {
                    throw ClientException("Following topics does not exist: $notExistingTopics")
                }

                topics
                    .asSequence()
                    .filter { it.name !in mirroredTopics }
                    .forEach { topicMetadata ->
                        mirroredTopics[topicMetadata.name] =
                            topicMetadata.partitions
                                .associate {
                                    it.id to MirroredPartition(
                                        startingOffset = it.latestOffset - 1,
                                        currentOffset = -1
                                    )
                                }

                        mirrorTopic(topicMetadata.name)
                    }
            }
            .let { Mono.empty() }

    private fun mirrorTopic(topic: String) {
        createTable(topic)
            .doOnNext { log.info("Table for topic $topic has been created") }
            .then(connection)
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
                        conn.insert(event)
                            .thenReturn(event)
                    }
                    .doOnNext {
                        mirroredTopics[it.topic()]!![it.partition()]!!.currentOffset = it.offset()
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
            asSequence()
                .withIndex()
                .filter { it.value.uppercase() in KEYWORDS_BEFORE_TABLE_NAME }
                .map { it.index + 1 }
                .map { get(it) }
                .map { it.removePrefix("\"").removeSuffix("\"") }
                .toList()
        }

    private fun createTable(table: String): Mono<Void> =
        connection
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

        return "CREATE TABLE \"$table\" ( ${columns.joinToString(separator = ",")} )"
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
            Schema.Type.NULL -> TODO()
            else -> avroToDbType(type)
        }

    private fun avroToDbType(type: Schema.Type): String =
        when (type) {
            Schema.Type.STRING -> "VARCHAR(10000)"
            Schema.Type.INT -> "INTEGER"
            Schema.Type.LONG -> "BIGINT"
            Schema.Type.FLOAT -> "REAL"
            Schema.Type.DOUBLE -> "DOUBLE PRECISION"
            Schema.Type.BOOLEAN -> "BOOLEAN"
            else -> throw Exception("Unhandled type: $type")
        }

    private fun Connection.insert(event: AppEvent): Mono<Result> {
        val eventColumns = eventColumns(event.value())
        val columns = listOf("__partition", "__offset") + eventColumns.columns
        val values = columns.joinToString(",") { "?" }
        val result = createStatement(
            """
            INSERT INTO "${event.topic()}" (${columns.joinToString(",")})
            VALUES ($values)
            """.trimIndent()
        )

        result
            .bind(0, event.partition())
            .bind(1, event.offset())

        (0 until eventColumns.columns.size)
            .forEach { i ->
                result.bind(i + 2, eventColumns.rows[i])
            }

        return Mono.from(result.execute())
    }

    private fun eventColumns(event: KafkaEventPart): DbEvent =
        when (event.type) {
            STRING -> TODO()
            AVRO ->
                with(event.data as GenericData.Record) {
                    val columns = schema.fields.map { it.name() }
                    DbEvent(
                        columns = columns,
                        rows = columns.map {
                            when(val row = get(it)) {
                                is Utf8 -> row.toString()
                                else -> row
                            }
                        }
                    )
                }
            NIL -> DbEvent(columns = listOf(), rows = listOf())
        }

    private fun select(req: SqlStatementReq): Flux<SqlStatementRow> =
        connection
            .flatMap { conn ->
                conn.createStatement(req.sql)
                    .execute()
                    .let { Mono.from(it) }
            }
            .flatMapMany(::toRow)

    private fun toRow(result: Result): Flux<SqlStatementRow> =
        Flux.create { sink ->
            Flux.from(result.map { row, _ -> row })
                .doOnComplete { sink.complete() }
                .doOnError { sink.error(it) }
                .subscribe { row ->
                    val columns = row.metadata.columnMetadatas.map { it.name }
                    sink.next(columns.associateWith { row[it] })
                }
        }

    data class DbEvent(
        val columns: List<String>,
        val rows: List<Any?>
    )
}

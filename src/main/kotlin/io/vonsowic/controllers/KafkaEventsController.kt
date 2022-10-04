package io.vonsowic.controllers

import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.vonsowic.KafkaEvent
import io.vonsowic.KafkaEventCreateReq
import io.vonsowic.KafkaEventPart
import io.vonsowic.KafkaEventPartType
import io.vonsowic.services.*
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration


private val TOPIC_REGEX = Regex("^t(\\d+)\$")
private val PARTITION_OPTION_REGEX = Regex("^t(\\d+)p(\\d+)([el])\$")

@Suppress("unused")
@Controller("/api/events")
class KafkaEventsController(
    private val kafkaEventsService: KafkaEventsService,
    private val senderService: SenderService,
    private val metadataService: MetadataService
) {

    @Post
    fun sendEvent(@Body req: KafkaEventCreateReq): Mono<Void> =
        senderService.send(req)

    @Get
    fun poll(req: HttpRequest<Void>): Flux<KafkaEvent> =
        kafkaEventsService
            .poll(req.pollOptions())
            .map { record ->
                KafkaEvent(
                    topic = record.topic(),
                    partition = record.partition(),
                    key = record.key().toMapIfAvro(),
                    value = record.value().toMapIfAvro(),
                    headers = record.headers()
                        ?.filterNotNull()
                        ?.associate { header -> header.key() to (header.value()?.toString() ?: "") }
                        ?: mapOf()
                )
            }

    private fun HttpRequest<Void>.pollOptions(): PollOptions =
        this.parameters
            .asMap(String::class.java, String::class.java)
            .let { params ->
                val topicsByIndex =
                    params.entries
                        .filter { it.key.matches(TOPIC_REGEX) }
                        .associate {
                            TOPIC_REGEX.matchEntire(it.key)!!.groupValues[1].toInt() to it.value
                        }

                val topicToPartitionToRange =
                    topicsByIndex.values
                        .associateWith { mutableMapOf<Int, MutablePartitionRange>() }
                        .toMutableMap()

                metadataService.topicsMetadata(topicsByIndex.values)
                    .flatMap {
                        it.partitions
                            .map { partition ->
                                TopicPartition(it.name, partition.id) to partition.latestOffset
                            }
                    }
                    .forEach {
                        topicToPartitionToRange[it.first.topic()]!!
                            .computeIfAbsent(it.first.partition()) { MutablePartitionRange() }
                            .endOffset = it.second
                    }

                params.entries
                    .filter { it.key.matches(PARTITION_OPTION_REGEX) }
                    .forEach {
                        val match = PARTITION_OPTION_REGEX.matchEntire(it.key)!!.groupValues
                        val topicIndex = match[1].toInt()
                        val partition = match[2].toInt()
                        val isStarting = match[3] == "e"
                        val topic = topicsByIndex[topicIndex] ?: throw Exception("missing 't$topicIndex' query param")
                        val offset = it.value.toLong()
                        topicToPartitionToRange[topic]!!
                            .computeIfAbsent(partition) { MutablePartitionRange() }
                            .apply {
                                if (isStarting) {
                                    startOffset = offset
                                } else {
                                    endOffset = offset
                                }
                            }
                    }

                PollOptions(
                    topicOptions = topicToPartitionToRange.map {
                        val topic = it.key
                        PollOption(
                            topicName = topic,
                            partitionRange = it.value
                                .entries
                                .filter {
                                    with(it.value) {
                                        (startOffset ?: 0) != endOffset
                                    }
                                }
                                .associate { partitionEntry ->
                                    partitionEntry.key to partitionEntry.value.toPartitionRange()
                                }
                        )
                    },
                )
            }

}

private fun KafkaEventPart.toMapIfAvro(): KafkaEventPart =
    if (this.type == KafkaEventPartType.AVRO) {
        val avroData = this.data as GenericData.Record
        KafkaEventPart(
            type = KafkaEventPartType.AVRO,
            data = avroData
                .schema
                .fields
                .associate { field -> field.name() to avroData.value(field) }
        )
    } else {
        this
    }

private fun GenericData.Record.value(field: Schema.Field): Any? =
    when (val value = this[field.name()]) {
        is Utf8 -> value.toString()
        else -> value
    }


data class MutablePartitionRange(
    var startOffset: Long? = null,
    var endOffset: Long? = null,
) {
    fun toPartitionRange() =
        PartitionRange(
            startOffset = startOffset,
            endOffset = endOffset
        )
}
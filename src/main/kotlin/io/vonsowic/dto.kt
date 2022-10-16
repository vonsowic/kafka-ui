package io.vonsowic

data class SqlStatementReq(
    val sql: String
)

typealias SqlStatementRow = Map<String, Any?>

data class KafkaEventCreateReq(
    val topic: String,
    val partition: Int? = null,
    val event: KafkaEventCreate? = null
)

data class KafkaEventCreate(
    val key: KafkaEventPart,
    val value: KafkaEventPart,
    val headers: Map<String, String> = mapOf()
)

data class KafkaEvent(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val key: KafkaEventPart,
    val value: KafkaEventPart,
    val headers: Map<String, String> = mapOf()
)

data class KafkaEventPart(
    val data: Any,
    val type: KafkaEventPartType
) {
    companion object {
        val NIL = KafkaEventPart(data = 0, type = KafkaEventPartType.NIL)
    }
}

enum class KafkaEventPartType {
    STRING,
    AVRO,
    NIL
}

data class PartitionMetadata(
    val id: Int,
    val earliestOffset: Long,
    val latestOffset: Long
)

data class TopicMetadata(
    val name: String,
    val partitions: Collection<PartitionMetadata>
)

data class ListTopicItem(
    val name: String
)
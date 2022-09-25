package io.vonsowic

data class SqlStatementReq(
    val sql: String
)

typealias SqlStatementRow = List<Any?>

enum class KafkaEventCreateStrategy {
    // create event as is
    SIMPLE,
    // generate value based on provided type
    AUTOGENERATE
}

data class KafkaEventCreateReq(
    val topic: String,
    val partition: Int? = null,
    val strategy: KafkaEventCreateStrategy = KafkaEventCreateStrategy.SIMPLE,
    val event: KafkaEvent? = null
)

data class KafkaEvent(
    val key: KafkaEventPart,
    val value: KafkaEventPart,
    val headers: Map<String, String> = mapOf()
)

data class KafkaEventPart(
    val data: Any?,
    val type: KafkaEventPartType
)

enum class KafkaEventPartType {
    STRING,
    AVRO,
    NIL
}

data class PartitionMetadata(
    val id: Int
)

data class TopicMetadata(
    val name: String,
    val partitions: Collection<PartitionMetadata>
)

data class ListTopicItem(
    val name: String
)
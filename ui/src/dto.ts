export interface Topic {
    name: string
}

export interface KafkaEventPart {
    data: any
    type: 'STRING' | 'AVRO' | 'NIL'
}

export interface KafkaEvent {
    topic: string
    partition: number
    offset: number
    timestamp: number
    key: KafkaEventPart
    value: KafkaEventPart
    headers: Map<string, string>
}

export interface Topic {
    name: string,
    partitions: TopicPartition[]
}

export interface TopicPartition {
    id: number,
    earliestOffset: number,
    latestOffset: number
}
package io.vonsowic.utils

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.vonsowic.KafkaEventPart
import io.vonsowic.KafkaEventPartType
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

class DelegatingSerializer(
    private val stringSerializer: StringSerializer = StringSerializer(),
) : Serializer<KafkaEventPart> {

    override fun serialize(topic: String, data: KafkaEventPart?): ByteArray? {
        return when (data?.type) {
            null -> null
            KafkaEventPartType.STRING -> stringSerializer.serialize(topic, data.data as String)
            KafkaEventPartType.AVRO -> TODO()
            KafkaEventPartType.NIL -> null
        }
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        stringSerializer.configure(configs, isKey)
    }
}

class DelegatingDeserializer(
    private val stringDeserializer: StringDeserializer = StringDeserializer(),
    private var avroDeserializer: KafkaAvroDeserializer? = null,
) : Deserializer<KafkaEventPart> {
    override fun deserialize(topic: String, data: ByteArray): KafkaEventPart {
        if (avroDeserializer != null) {
            try {
                return KafkaEventPart(
                    type = KafkaEventPartType.AVRO,
                    data = avroDeserializer!!.deserialize(topic, data)
                )
            } catch (_: SerializationException) {
            }
        }

        return KafkaEventPart(
            type = KafkaEventPartType.STRING,
            data = stringDeserializer.deserialize(topic, data)
        )
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        stringDeserializer.configure(configs, isKey)
        if (configs.contains(SCHEMA_REGISTRY_URL_CONFIG)) {
            avroDeserializer =
                KafkaAvroDeserializer()
                    .apply { configure(configs, isKey) }
        }
    }
}

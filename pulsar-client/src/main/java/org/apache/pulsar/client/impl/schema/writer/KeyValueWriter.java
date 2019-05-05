package org.apache.pulsar.client.impl.schema.writer;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import java.nio.ByteBuffer;

public class KeyValueWriter<K, V> implements SchemaWriter<KeyValue<K, V>> {

    private final Schema<K> keySchema;

    private final Schema<V> valueSchema;

    private final KeyValueEncodingType keyValueEncodingType;

    public KeyValueWriter(Schema<K> keySchema, Schema<V> valueSchema, KeyValueEncodingType keyValueEncodingType) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.keyValueEncodingType = keyValueEncodingType;
    }

    @Override
    public byte[] write(KeyValue<K, V> message) {
        if (keyValueEncodingType != null && keyValueEncodingType == KeyValueEncodingType.INLINE) {
            byte [] keyBytes = keySchema.encode(message.getKey());
            byte [] valueBytes = valueSchema.encode(message.getValue());
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + valueBytes.length);
            byteBuffer.putInt(keyBytes.length).put(keyBytes).putInt(valueBytes.length).put(valueBytes);
            return byteBuffer.array();
        } else {
            return valueSchema.encode(message.getValue());
        }
    }
}

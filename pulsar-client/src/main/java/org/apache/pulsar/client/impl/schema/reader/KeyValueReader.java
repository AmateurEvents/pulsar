package org.apache.pulsar.client.impl.schema.reader;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import java.nio.ByteBuffer;

public class KeyValueReader<K, V> implements SchemaReader<KeyValue<K, V>> {

    private final Schema<K> keySchema;

    private final Schema<V> valueSchema;

    private final KeyValueEncodingType keyValueEncodingType;

    public KeyValueReader(Schema<K> keySchema, Schema<V> valueSchema, KeyValueEncodingType keyValueEncodingType) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.keyValueEncodingType = keyValueEncodingType;
    }

    @Override
    public KeyValue<K, V> read(byte[] bytes) {
        if (keyValueEncodingType == KeyValueEncodingType.SEPARATED) {
            throw new SchemaSerializationException("This method cannot be used under this SEPARATED encoding type");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength = byteBuffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        byteBuffer.get(keyBytes);

        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuffer.get(valueBytes);
        return read(keyBytes, valueBytes);
    }

    @Override
    public KeyValue<K, V> read(byte[] keyBytes, byte[] valueBytes) {
        return new KeyValue<>(keySchema.decode(keyBytes), valueSchema.decode(valueBytes));
    }
}

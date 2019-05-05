/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.schema;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import lombok.Getter;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.reader.KeyValueReader;
import org.apache.pulsar.client.impl.schema.writer.KeyValueWriter;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;


/**
 * [Key, Value] pair schema definition
 */
public class KeyValueSchema<K, V> extends StructSchema<KeyValue<K, V>> {

    @Getter
    private final Schema<K> keySchema;
    @Getter
    private final Schema<V> valueSchema;

    @Getter
    private final KeyValueEncodingType keyValueEncodingType;

    /**
     * Key Value Schema using passed in schema type, support JSON and AVRO currently.
     */
    public static <K, V> Schema<KeyValue<K, V>> of(Class<K> key, Class<V> value, SchemaType type) {
        checkArgument(SchemaType.JSON == type || SchemaType.AVRO == type);
        if (SchemaType.JSON == type) {
            return new KeyValueSchema<>(parseKeyValueSchemaInfo(
                    JSONSchema.of(key), JSONSchema.of(value), KeyValueEncodingType.INLINE),
                    JSONSchema.of(key), JSONSchema.of(value), KeyValueEncodingType.INLINE);
        } else {
            // AVRO
            return new KeyValueSchema<>(parseKeyValueSchemaInfo(
                    AvroSchema.of(key), AvroSchema.of(value), KeyValueEncodingType.INLINE),
                    AvroSchema.of(key), AvroSchema.of(value), KeyValueEncodingType.INLINE);
        }
    }


    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema, Schema<V> valueSchema) {

        return new KeyValueSchema<>(parseKeyValueSchemaInfo(keySchema, valueSchema, KeyValueEncodingType.INLINE),
                keySchema, valueSchema, KeyValueEncodingType.INLINE);
    }

    public static <K, V> Schema<KeyValue<K, V>> of(Schema<K> keySchema,
                                                   Schema<V> valueSchema,
                                                   KeyValueEncodingType keyValueEncodingType) {
        return new KeyValueSchema<>(parseKeyValueSchemaInfo(keySchema, valueSchema, keyValueEncodingType),
                keySchema, valueSchema, keyValueEncodingType);
    }

    private static final Schema<KeyValue<byte[], byte[]>> KV_BYTES = new KeyValueSchema<>(
            parseKeyValueSchemaInfo(BytesSchema.of(), BytesSchema.of(), KeyValueEncodingType.INLINE),
            BytesSchema.of(), BytesSchema.of(), KeyValueEncodingType.INLINE);

    public static Schema<KeyValue<byte[], byte[]>> kvBytes() {
        return KV_BYTES;
    }

    private KeyValueSchema(SchemaInfo schemaInfo, Schema<K> keySchema, Schema<V> valueSchema, KeyValueEncodingType keyValueEncodingType) {
        super(schemaInfo);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.keyValueEncodingType = keyValueEncodingType;
        setReader(new KeyValueReader<>(keySchema, valueSchema, keyValueEncodingType));
        setWriter(new KeyValueWriter<>(keySchema, valueSchema, keyValueEncodingType));
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    protected static SchemaInfo parseKeyValueSchemaInfo(Schema keySchema, Schema valueSchema, KeyValueEncodingType keyValueEncodingType) {
        byte[] keySchemaInfo = keySchema.getSchemaInfo().getSchema();
        byte[] valueSchemaInfo = valueSchema.getSchemaInfo().getSchema();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + keySchemaInfo.length + 4 + valueSchemaInfo.length);
        byteBuffer.putInt(keySchemaInfo.length).put(keySchemaInfo)
                .putInt(valueSchemaInfo.length).put(valueSchemaInfo);

        Map<String, String> properties = Maps.newHashMap();

        properties.put("key.schema.name", keySchema.getSchemaInfo().getName());
        properties.put("key.schema.type", String.valueOf(keySchema.getSchemaInfo().getType()));
        Gson keySchemaGson = new Gson();
        properties.put("key.schema.properties", keySchemaGson.toJson(keySchema.getSchemaInfo().getProperties()));
        properties.put("value.schema.name", valueSchema.getSchemaInfo().getName());
        properties.put("value.schema.type", String.valueOf(valueSchema.getSchemaInfo().getType()));
        Gson valueSchemaGson = new Gson();
        properties.put("value.schema.properties", valueSchemaGson.toJson(valueSchema.getSchemaInfo().getProperties()));

        checkNotNull(keyValueEncodingType, "Null encoding type is provided");
        properties.put("kv.encoding.type", String.valueOf(keyValueEncodingType));

        return SchemaInfo.builder()
                .schema(byteBuffer.array())
                .properties(properties)
                .name("KeyValue")
                .type(SchemaType.KEY_VALUE).build();
    }

    @Override
    protected SchemaReader<KeyValue<K, V>> loadReader(byte[] schemaVersion) {
        if (this.schemaInfo.getProperties().get("key.schema.type") == String.valueOf(SchemaType.AVRO)
                && this.schemaInfo.getProperties().get("value.schema.type") == String.valueOf(SchemaType.AVRO)) {
            SchemaInfo schemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion);
            if (schemaInfo != null) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(schemaInfo.getSchema());
                int keySchemaLength = byteBuffer.getInt();
                int valueSchemaLength = byteBuffer.getInt();
                byte[] keyBytes = new byte[keySchemaLength];
                byteBuffer.get(keyBytes);
                byte[] valueBytes = new byte[valueSchemaLength];
                byteBuffer.get(valueBytes);
                Schema<K> keySchema = AvroSchema.of(SchemaDefinition.<K>builder().withJsonDef(new String(keyBytes)).build());
                Schema<V> valueSchema = AvroSchema.of(SchemaDefinition.<V>builder().withJsonDef(new String(valueBytes)).build());
                return new KeyValueReader<>(keySchema, valueSchema,
                        KeyValueEncodingType.valueOf(schemaInfo.getProperties().get("kv.encoding.type")));
            }
        }
        return reader;
    }
}

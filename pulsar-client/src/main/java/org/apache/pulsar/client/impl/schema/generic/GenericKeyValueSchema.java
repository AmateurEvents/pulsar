package org.apache.pulsar.client.impl.schema.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import lombok.Getter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;

public class GenericKeyValueSchema extends GenericSchemaImpl {

    private GenericJsonSchema keyGenericJsonSchema;
    private GenericJsonSchema valueGenericJsonSchema;
    private GenericAvroSchema keyGenericAvroSchema;
    private GenericAvroSchema valueGenericAvroSchema;

    public GenericKeyValueSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);

        Gson keySchemaGson = new Gson();
        Gson valueSchemaGson = new Gson();
        if (keySchemaType == SchemaType.JSON && valueSchemaType == SchemaType.JSON) {
            SchemaInfo jsonKeySchemaInfo = new SchemaInfo()
                    .setName("json")
                    .setType(SchemaType.JSON)
                    .setSchema(keySchema)
                    .setProperties(keySchemaGson.fromJson(new String(keySchemaProperties), Map.class));
            SchemaInfo jsonValueSchemaInfo = new SchemaInfo()
                    .setName("json")
                    .setType(SchemaType.JSON)
                    .setSchema(keySchema)
                    .setProperties(valueSchemaGson.fromJson(new String(valueSchemaProperties), Map.class));
            keyGenericJsonSchema = new GenericJsonSchema(jsonKeySchemaInfo);
            valueGenericJsonSchema = new GenericJsonSchema(jsonValueSchemaInfo);
        } else {
            // AVRO
            SchemaInfo avroKeySchemaInfo = new SchemaInfo()
                    .setName("avro")
                    .setType(SchemaType.AVRO)
                    .setSchema(keySchema)
                    .setProperties(keySchemaGson.fromJson(new String(keySchemaProperties), Map.class));
            SchemaInfo avroValueSchemaInfo = new SchemaInfo()
                    .setName("avro")
                    .setType(SchemaType.AVRO)
                    .setSchema(keySchema)
                    .setProperties(valueSchemaGson.fromJson(new String(valueSchemaProperties), Map.class));
            keyGenericAvroSchema = new GenericAvroSchema(avroKeySchemaInfo);
            valueGenericAvroSchema = new GenericAvroSchema(avroValueSchemaInfo);
        }
    }

    @Override
    public byte[] encode(GenericRecord message) {
        byte [] keyBytes;
        byte [] valueBytes;
        ByteBuffer byteBuffer;
        GenericKeyValueRecord genericKeyValueRecord = (GenericKeyValueRecord) message;
        if (keySchemaType == SchemaType.JSON && valueSchemaType == SchemaType.JSON) {
            keyBytes = this.keyGenericJsonSchema.encode((GenericJsonRecord)genericKeyValueRecord.getField("key"));
            valueBytes = this.valueGenericJsonSchema.encode((GenericJsonRecord)genericKeyValueRecord.getField("value"));
        } else {
            keyBytes = this.keyGenericAvroSchema.encode((GenericAvroRecord)genericKeyValueRecord.getField("key"));
            valueBytes = this.valueGenericAvroSchema.encode((GenericAvroRecord)genericKeyValueRecord.getField("value"));
        }
        byteBuffer = ByteBuffer.allocate(4 + keyBytes.length + 4 + valueBytes.length);
        byteBuffer.putInt(keyBytes.length).put(keyBytes).putInt(valueBytes.length).put(valueBytes);
        byteBuffer = ByteBuffer.allocate(4 + valueBytes.length);
        byteBuffer.putInt(valueBytes.length).put(valueBytes);

        return byteBuffer.array();
    }

    @Override
    public GenericRecord decode(byte[] bytes, byte[] schemaVersion) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int keyLength;
        byte [] keyBytes = {};
        keyLength = byteBuffer.getInt();
        keyBytes = new byte[keyLength];
        byteBuffer.get(keyBytes);

        int valueLength = byteBuffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        byteBuffer.get(valueBytes);
        if (keySchemaType == SchemaType.JSON && valueSchemaType == SchemaType.JSON) {
            return new GenericKeyValueRecord(
                    schemaVersion, fields,
                    new KeyValue(this.keyGenericJsonSchema.decode(keyBytes),
                            this.valueGenericJsonSchema.decode(valueBytes)));
        } else {
            return new GenericKeyValueRecord(
                    schemaVersion, fields,
                    new KeyValue(this.keyGenericAvroSchema.decode(keyBytes),
                            this.valueGenericAvroSchema.decode(valueBytes)));
        }
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        if (this.schemaInfo.getType() == SchemaType.JSON) {
            throw new UnsupportedOperationException("Json Schema doesn't support record builder yet");
        } else {
            return new AvroRecordBuilderImpl(this);
        }
    }
}

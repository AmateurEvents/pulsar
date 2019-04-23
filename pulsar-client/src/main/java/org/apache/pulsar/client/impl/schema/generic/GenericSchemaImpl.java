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
package org.apache.pulsar.client.impl.schema.generic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A generic schema representation.
 */
public abstract class GenericSchemaImpl implements GenericSchema {

    protected final org.apache.avro.Schema schema;
    protected final byte[] keySchema;
    protected final byte[] valueSchema;
    protected final byte[] keySchemaProperties;
    protected final byte[] valueSchemaProperties;
    protected SchemaType keySchemaType;
    protected SchemaType valueSchemaType;
    protected final List<Field> fields;
    protected final SchemaInfo schemaInfo;

    protected GenericSchemaImpl(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
        if (schemaInfo.getType() == SchemaType.KEY_VALUE) {
            byte [] bytes = schemaInfo.getSchema();
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            int keySchemaInfoLength = byteBuffer.getInt();
            keySchema = new byte[keySchemaInfoLength];
            byteBuffer.get(keySchema);
            int keySchemaNameLength = byteBuffer.getInt();
            byte[] keySchemaName = new byte[keySchemaNameLength];
            byteBuffer.get(keySchemaName);
            int keySchemaTypeLength = byteBuffer.getInt();
            byte[] keySchemaType = new byte[keySchemaTypeLength];
            byteBuffer.get(keySchemaType);
            int keySchemaPropertiesLength = byteBuffer.getInt();
            keySchemaProperties = new byte[keySchemaPropertiesLength];
            byteBuffer.get(keySchemaProperties);
            int valueSchemaInfoLength = byteBuffer.getInt();
            valueSchema = new byte[valueSchemaInfoLength];
            byteBuffer.get(valueSchema);
            int valueSchemaNameLength = byteBuffer.getInt();
            byte[] valueSchemaName = new byte[valueSchemaNameLength];
            byteBuffer.get(valueSchemaName);
            int valueSchemaTypeLength = byteBuffer.getInt();
            byte[] valueSchemaType = new byte[valueSchemaTypeLength];
            byteBuffer.get(valueSchemaType);
            int valueSchemaPropertiesLength = byteBuffer.getInt();
            valueSchemaProperties = new byte[valueSchemaPropertiesLength];
            byteBuffer.get(valueSchemaProperties);
            this.schema = null;
            this.fields = new org.apache.avro.Schema.Parser().parse(
                    new String(keySchema, UTF_8)
            ).getFields().stream().map(f -> new Field(f.name(), f.pos())).collect(Collectors.toList());
            this.fields.addAll(new org.apache.avro.Schema.Parser().parse(
                    new String(valueSchema, UTF_8)).getFields().stream()
                    .map(f -> new Field(f.name(), f.pos()))
                    .collect(Collectors.toList()));
        } else {
            this.keySchema = null;
            this.valueSchema = null;
            this.keySchemaProperties = null;
            this.valueSchemaProperties = null;
            this.schema = new org.apache.avro.Schema.Parser().parse(
                    new String(schemaInfo.getSchema(), UTF_8)
            );
            this.fields = schema.getFields()
                    .stream()
                    .map(f -> new Field(f.name(), f.pos()))
                    .collect(Collectors.toList());
        }


    }

    public org.apache.avro.Schema getAvroSchema() {
        return schema;
    }

    public byte[] getKeySchema() {
        return keySchema;
    }

    public byte[] getValueSchema() {
        return valueSchema;
    }

    @Override
    public List<Field> getFields() {
        return fields;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    /**
     * Create a generic schema out of a <tt>SchemaInfo</tt>.
     *
     * @param schemaInfo schema info
     * @return a generic schema instance
     */
    public static GenericSchemaImpl of(SchemaInfo schemaInfo) {
        switch (schemaInfo.getType()) {
            case AVRO:
                return new GenericAvroSchema(schemaInfo);
            case JSON:
                return new GenericJsonSchema(schemaInfo);
            case KEY_VALUE:
                return new GenericKeyValueSchema(schemaInfo);
            default:
                throw new UnsupportedOperationException("Generic schema is not supported on schema type '"
                    + schemaInfo.getType() + "'");
        }
    }

}

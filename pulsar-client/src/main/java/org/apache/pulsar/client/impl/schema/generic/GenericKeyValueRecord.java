package org.apache.pulsar.client.impl.schema.generic;

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;

public class GenericKeyValueRecord extends VersionedGenericRecord {


    private KeyValue<Object, Object> keyValue;

    GenericKeyValueRecord(byte[] schemaVersion,
                          List<Field> fields,
                          KeyValue<Object, Object> keyValue) {
        super(schemaVersion, fields);
        this.keyValue = keyValue;
    }

    @Override
    public Object getField(String fieldName) {
        if (fieldName.equals("key")) {
            return keyValue.getKey();
        } else {
            return keyValue.getValue();
        }
    }
}

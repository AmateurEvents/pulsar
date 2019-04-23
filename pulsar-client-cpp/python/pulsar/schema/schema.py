#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


from abc import abstractmethod
import json
import fastavro
import _pulsar
import io


class Schema(object):
    def __init__(self, record_cls, schema_type, schema_definition, schema_name):
        self._record_cls = record_cls
        self._schema_info = _pulsar.SchemaInfo(schema_type, schema_name,
                                               json.dumps(schema_definition, indent=True))

    @abstractmethod
    def encode(self, obj):
        pass

    @abstractmethod
    def decode(self, data):
        pass

    def schema_info(self):
        return self._schema_info

    def _validate_object_type(self, obj):
        if not isinstance(obj, self._record_cls):
            raise TypeError('Invalid record obj of type ' + str(type(obj))
                            + ' - expected type is ' + str(self._record_cls))


class BytesSchema(Schema):
    def __init__(self):
        super(BytesSchema, self).__init__(bytes, _pulsar.SchemaType.BYTES, None, 'BYTES')

    def encode(self, data):
        self._validate_object_type(data)
        return data

    def decode(self, data):
        return data


class StringSchema(Schema):
    def __init__(self):
        super(StringSchema, self).__init__(str, _pulsar.SchemaType.STRING, None, 'STRING')

    def encode(self, obj):
        self._validate_object_type(obj)
        return obj.encode('utf-8')

    def decode(self, data):
        return data.decode('utf-8')


class JsonSchema(Schema):

    def __init__(self, record_cls):
        super(JsonSchema, self).__init__(record_cls, _pulsar.SchemaType.JSON,
                                         record_cls.schema(), 'JSON')

    def encode(self, obj):
        self._validate_object_type(obj)
        return json.dumps(obj.__dict__, default=lambda o: o.__dict__, indent=True).encode('utf-8')

    def decode(self, data):
        return self._record_cls(**json.loads(data))


class AvroSchema(Schema):
    def __init__(self, record_cls):
        super(AvroSchema, self).__init__(record_cls, _pulsar.SchemaType.AVRO,
                                         record_cls.schema(), 'AVRO')
        self._schema = record_cls.schema()

    def encode(self, obj):
        self._validate_object_type(obj)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, self._schema, obj.__dict__)
        return buffer.getvalue()

    def decode(self, data):
        buffer = io.BytesIO(data)
        d = fastavro.schemaless_reader(buffer, self._schema)
        return self._record_cls(**d)


class KeyValueSchema(Schema):
    def __init__(self, key_record_cls, value_record_cls):
        super(KeyValueSchema, self).__init__(key_record_cls, _pulsar.SchemaType.KEY_VALUE,
                                            None, 'KEY_VALUE')
        self._key_schema = key_record_cls.schema()
        self._value_schema = value_record_cls.schema()

    def encode(self, obj):
        buffer = io.BytesIO()
        buffer.write(len(obj.key) + obj.key.encode(encoding='utf-8') + len(obj.value) + obj.value.encode('utf-8'))
        return buffer

    def decode(self, data):
        buffer = io.BytesIO(data)
        message = buffer.getvalue()
        message_key_length = message[0:4]
        message_key_value = message[4:message_key_length]
        message_value_length = message[4 + message_key_length: 4 + message_key_length + 4]
        message_value = message[4 + message_key_length + 4: 4 + message_key_length + 4 + message_value_length]
        return self._record_cls(**{'key': message_key_value, 'value': message_value })
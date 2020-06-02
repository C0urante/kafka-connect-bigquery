package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever.integration;

/*
 * Copyright 2016 WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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


import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever.SchemaRegistrySchemaRetriever;
import com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever.SchemaRegistrySchemaRetrieverConfig;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SchemaRegistrySchemaRetrieverIT extends ClusterTestHarness {

  public SchemaRegistrySchemaRetrieverIT() {
    super(1, true);
  }

  @Test
  public void testSchemaRegistrySchemaRetriever() throws Exception {
    final String topic = "test-topic";
    final Schema avroKeySchema = SchemaBuilder
        .record("testrecordkey")
        .namespace("com.wepay.namespace")
        .fields()
          .requiredString("k")
        .endRecord();
    final Schema avroValueSchema = SchemaBuilder
        .record("testrecordvalue")
        .namespace("com.wepay.namespace")
        .fields()
          .requiredBytes("f1")
          .optionalBoolean("f2")
          .name("f3").type().doubleType().doubleDefault(0.42)
        .endRecord();

    SchemaRegistryClient client =
        new CachedSchemaRegistryClient("http://localhost:" + schemaRegistryPort, 10);
    client.register(topic + "-key", new AvroSchema(avroKeySchema));
    client.register(topic + "-value", new AvroSchema(avroValueSchema));

    Map<String, String> retrieverProps = new HashMap<>();
    retrieverProps.put(SchemaRegistrySchemaRetrieverConfig.LOCATION_CONFIG, "http://localhost:" + schemaRegistryPort);
    SchemaRegistrySchemaRetriever retriever = new SchemaRegistrySchemaRetriever();
    retriever.configure(retrieverProps);

    org.apache.kafka.connect.data.Schema expectedKeySchema =
        org.apache.kafka.connect.data.SchemaBuilder.struct()
          .name("com.wepay.namespace.testrecordkey")
          .field("k", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();
    org.apache.kafka.connect.data.Schema expectedValueSchema =
        org.apache.kafka.connect.data.SchemaBuilder.struct()
          .name("com.wepay.namespace.testrecordvalue")
          .field("f1", org.apache.kafka.connect.data.Schema.BYTES_SCHEMA)
          .field("f2", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("f3", org.apache.kafka.connect.data.SchemaBuilder
              .float64().defaultValue(0.42).build())
        .build();

    org.apache.kafka.connect.data.Schema actualKeySchema =
        retriever.retrieveSchema(TableId.of("d", "t"), topic, KafkaSchemaRecordType.KEY);
    org.apache.kafka.connect.data.Schema actualValueSchema =
        retriever.retrieveSchema(TableId.of("d", "t"), topic, KafkaSchemaRecordType.VALUE);

    assertEquals(expectedKeySchema, actualKeySchema);
    assertEquals(expectedValueSchema, actualValueSchema);

    client.deleteSubject(topic + "-key");
    try {
      retriever.retrieveSchema(TableId.of("d", "t"), topic, KafkaSchemaRecordType.KEY);
      fail("Should have thrown exception");
    } catch (ConnectException e) {
      // Expected
    }

    actualValueSchema =
        retriever.retrieveSchema(TableId.of("d", "t"), topic, KafkaSchemaRecordType.VALUE);
    assertEquals(expectedValueSchema, actualValueSchema);
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema.Parser;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertThrows;

/** Tests for the {@link ApicurioRegistryAvroFormatFactory}. */
class ApicurioRegistryAvroFormatFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    private static final RowType ROW_TYPE =
            (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();

    private static final String SUBJECT = "test-subject";
    private static final String REGISTRY_URL = "http://localhost:8081";
    private static final String SCHEMA_STRING =
            "{\n"
                    + "    \"type\": \"record\",\n"
                    + "    \"name\": \"test_record\",\n"
                    + "    \"fields\": [\n"
                    + "        {\n"
                    + "            \"name\": \"a\",\n"
                    + "            \"type\": [\n"
                    + "                \"null\",\n"
                    + "                \"string\"\n"
                    + "            ],\n"
                    + "            \"default\": null\n"
                    + "        },\n"
                    + "        {\n"
                    + "            \"name\": \"b\",\n"
                    + "            \"type\": [\n"
                    + "                \"null\",\n"
                    + "                \"int\"\n"
                    + "            ],\n"
                    + "            \"default\": null\n"
                    + "        },\n"
                    + "        {\n"
                    + "            \"name\": \"c\",\n"
                    + "            \"type\": [\n"
                    + "                \"null\",\n"
                    + "                \"boolean\"\n"
                    + "            ],\n"
                    + "            \"default\": null\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}";

    private static final Map<String, String> EXPECTED_OPTIONAL_PROPERTIES = new HashMap<>();

    static {
        EXPECTED_OPTIONAL_PROPERTIES.put(
                AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        EXPECTED_OPTIONAL_PROPERTIES.put(
                AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD.key(), "123456");
        EXPECTED_OPTIONAL_PROPERTIES.put(
                AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        EXPECTED_OPTIONAL_PROPERTIES.put(
                AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD.key(), "123456");
        EXPECTED_OPTIONAL_PROPERTIES.put(
                AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE.key(), "USER_INFO");
        EXPECTED_OPTIONAL_PROPERTIES.put(
                AvroApicurioFormatOptions.BASIC_AUTH_USER_INFO.key(), "user:pwd");
        EXPECTED_OPTIONAL_PROPERTIES.put(
                AvroApicurioFormatOptions.BEARER_AUTH_TOKEN.key(), "CUSTOM");
        EXPECTED_OPTIONAL_PROPERTIES.put(AvroApicurioFormatOptions.ENABLE_HEADERS.key(), "True");

        //
        //
        //        final Map<String, Object> properties = new HashMap<>();
        //
        //        EXPECTED_OPTIONAL_PROPERTIES.put(
        //               AvroApicurioFormatOptions.PROPERTIES.key(), null);
        //        EXPECTED_OPTIONAL_PROPERTIES.put(
        //                AvroApicurioFormatOptions.ENABLE_HEADERS.key(),"True");
        //
        //                .getOptional(AvroApicurioFormatOptions.HEADERS_HANDLER)
        //                .ifPresent(v -> properties.put(SerdeConfig.HEADERS_HANDLER, v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.ID_HANDLER)
        //                .ifPresent(v -> properties.put(SerdeConfig.ID_HANDLER, v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER)
        //                .ifPresent(v -> properties.put(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER,
        // v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION)
        //                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.location",
        // v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD)
        //                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.password",
        // v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION)
        //                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.location",
        // v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD)
        //                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.password",
        // v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE)
        //                .ifPresent(v -> properties.put("basic.auth.credentials.source", v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.BASIC_AUTH_USER_INFO)
        //                .ifPresent(v -> properties.put("basic.auth.user.info", v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE)
        //                .ifPresent(v -> properties.put("bearer.auth.credentials.source", v));
        //        formatOptions
        //                .getOptional(AvroApicurioFormatOptions.BEARER_AUTH_TOKEN)
        //                .ifPresent(v -> properties.put("bearer.auth.token", v));

    }

    @Test
    void testDeserializationSchema() {
        final AvroRowDataDeserializationSchema expectedDeser =
                new AvroRowDataDeserializationSchema(
                        ApicurioRegistryAvroDeserializationSchema.forGeneric(
                                AvroSchemaConverter.convertToSchema(ROW_TYPE), REGISTRY_URL),
                        AvroToRowDataConverters.createRowConverter(ROW_TYPE),
                        InternalTypeInfo.of(ROW_TYPE));

        final DynamicTableSource actualSource = createTableSource(SCHEMA, getDefaultOptions());
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    @Test
    void testSerializationSchema() {
        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(
                        ROW_TYPE,
                        ApicurioRegistryAvroSerializationSchema.forGeneric(
                                SUBJECT,
                                AvroSchemaConverter.convertToSchema(ROW_TYPE),
                                REGISTRY_URL),
                        RowDataToAvroConverters.createConverter(ROW_TYPE));

        final DynamicTableSink actualSink = createTableSink(SCHEMA, getDefaultOptions());
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        assertThat(actualSer).isEqualTo(expectedSer);
    }

    @Test
    void testMissingSubjectForSink() {
        final Map<String, String> options =
                getModifiedOptions(opts -> opts.remove("avro-Apicurio.subject"));

        assertThatThrownBy(() -> createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Option avro-Apicurio.subject is required for serialization"));
    }

    @Test
    void testDeserializationSchemaWithOptionalProperties() {
        final AvroRowDataDeserializationSchema expectedDeser =
                new AvroRowDataDeserializationSchema(
                        ApicurioRegistryAvroDeserializationSchema.forGeneric(
                                new Parser().parse(SCHEMA_STRING),
                                REGISTRY_URL,
                                EXPECTED_OPTIONAL_PROPERTIES),
                        AvroToRowDataConverters.createRowConverter(ROW_TYPE),
                        InternalTypeInfo.of(ROW_TYPE));

        final DynamicTableSource actualSource = createTableSource(SCHEMA, getOptionalProperties());
        assertThat(actualSource).isInstanceOf(TestDynamicTableFactory.DynamicTableSourceMock.class);
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

        DeserializationSchema<RowData> actualDeser =
                scanSourceMock.valueFormat.createRuntimeDecoder(
                        ScanRuntimeProviderContext.INSTANCE, SCHEMA.toPhysicalRowDataType());

        assertThat(actualDeser).isEqualTo(expectedDeser);
    }

    @Test
    void testSerializationSchemaWithOptionalProperties() {
        final AvroRowDataSerializationSchema expectedSer =
                new AvroRowDataSerializationSchema(
                        ROW_TYPE,
                        ApicurioRegistryAvroSerializationSchema.forGeneric(
                                SUBJECT,
                                new Parser().parse(SCHEMA_STRING),
                                REGISTRY_URL,
                                EXPECTED_OPTIONAL_PROPERTIES),
                        RowDataToAvroConverters.createConverter(ROW_TYPE));

        final DynamicTableSink actualSink = createTableSink(SCHEMA, getOptionalProperties());
        assertThat(actualSink).isInstanceOf(TestDynamicTableFactory.DynamicTableSinkMock.class);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(null, SCHEMA.toPhysicalRowDataType());

        assertThat(actualSer).isEqualTo(expectedSer);
    }

    @Test
    public void testSerializationSchemaWithInvalidOptionalSchema() {
        Map<String, String> optionalProperties = getOptionalProperties();
        optionalProperties.put("avro-Apicurio.schema", SCHEMA_STRING.replace("int", "string"));

        final DynamicTableSink actualSink = createTableSink(SCHEMA, optionalProperties);
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        sinkMock.valueFormat.createRuntimeEncoder(
                                null, SCHEMA.toPhysicalRowDataType()));
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getDefaultOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getDefaultOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");

        options.put("format", ApicurioRegistryAvroFormatFactory.IDENTIFIER);
        options.put("avro-Apicurio.subject", SUBJECT);
        options.put("avro-Apicurio.url", REGISTRY_URL);
        return options;
    }

    private Map<String, String> getOptionalProperties() {
        final Map<String, String> properties = new HashMap<>();
        // defined via Flink maintained options
        properties.put(
                AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        properties.put(AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD.key(), "123456");
        properties.put(
                AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION.key(),
                getAbsolutePath("/test-keystore.jks"));
        properties.put(AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD.key(), "123456");
        properties.put(AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE.key(), "USER_INFO");
        properties.put(AvroApicurioFormatOptions.BASIC_AUTH_USER_INFO.key(), "user:pwd");
        // defined via general property map
        properties.put("properties.bearer.auth.token", "CUSTOM");
        properties.put("schema", SCHEMA_STRING);

        return getModifiedOptions(
                opts ->
                        properties.forEach(
                                (k, v) ->
                                        opts.put(
                                                String.format(
                                                        "%s.%s",
                                                        ApicurioRegistryAvroFormatFactory
                                                                .IDENTIFIER,
                                                        k),
                                                v)));
    }

    private static String getAbsolutePath(String path) {
        try {
            return ApicurioRegistryAvroFormatFactoryTest.class.getResource(path).toURI().getPath();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}

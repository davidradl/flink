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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Table format factory for providing configured instances of Schema Registry Avro to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
@Internal
public class ApicurioRegistryAvroFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "avro-apicurio";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        String schemaRegistryURL = formatOptions.get(AvroApicurioFormatOptions.URL);
        Optional<String> schemaString = formatOptions.getOptional(AvroApicurioFormatOptions.SCHEMA);
        Map<String, Object> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);

        return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType,
                    int[][] projections) {
                producedDataType = Projection.of(projections).project(producedDataType);
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final Schema schema =
                        schemaString
                                .map(s -> getAvroSchema(s, rowType))
                                .orElse(AvroSchemaConverter.convertToSchema(rowType));
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new AvroRowDataDeserializationSchema(
                        ApicurioRegistryAvroDeserializationSchema.forGeneric(
                                schema, schemaRegistryURL, optionalPropertiesMap),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        rowDataTypeInfo);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        String schemaRegistryURL = formatOptions.get(AvroApicurioFormatOptions.URL);
        Optional<String> subject = formatOptions.getOptional(AvroApicurioFormatOptions.SUBJECT);
        Optional<String> schemaString = formatOptions.getOptional(AvroApicurioFormatOptions.SCHEMA);
        Map<String, Object> optionalPropertiesMap = buildOptionalPropertiesMap(formatOptions);

        if (!subject.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Option %s.%s is required for serialization",
                            IDENTIFIER, AvroApicurioFormatOptions.SUBJECT.key()));
        }

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                final Schema schema =
                        schemaString
                                .map(s -> getAvroSchema(s, rowType))
                                .orElse(AvroSchemaConverter.convertToSchema(rowType));
                Map<String, Object> optionalPropertiesMapObjectValue = new HashMap<>();
                // the constructor below expects a Map<String, Object> not a Map<String, String>
                for (String key : optionalPropertiesMap.keySet()) {
                    optionalPropertiesMapObjectValue.put(
                            key, optionalPropertiesMapObjectValue.get(key));
                }

                return new AvroRowDataSerializationSchema(
                        rowType,
                        ApicurioRegistryAvroSerializationSchema.forGeneric(
                                subject.get(),
                                schema,
                                schemaRegistryURL,
                                optionalPropertiesMapObjectValue),
                        RowDataToAvroConverters.createConverter(rowType));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AvroApicurioFormatOptions.URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AvroApicurioFormatOptions.SUBJECT);
        options.add(AvroApicurioFormatOptions.SCHEMA);
        options.add(AvroApicurioFormatOptions.PROPERTIES);
        options.add(AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION);
        options.add(AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD);
        options.add(AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION);
        options.add(AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD);
        options.add(AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE);
        options.add(AvroApicurioFormatOptions.BASIC_AUTH_USER_INFO);
        options.add(AvroApicurioFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE);
        options.add(AvroApicurioFormatOptions.BEARER_AUTH_TOKEN);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(AvroApicurioFormatOptions.URL).collect(Collectors.toSet());
    }

    public static @Nullable Map<String, Object> buildOptionalPropertiesMap(
            ReadableConfig formatOptions) {
        final Map<String, Object> properties = new HashMap<>();

        formatOptions
                .getOptional(AvroApicurioFormatOptions.PROPERTIES)
                .ifPresent(properties::putAll);

        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_KEYSTORE_LOCATION)
                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.location", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_KEYSTORE_PASSWORD)
                .ifPresent(v -> properties.put("schema.registry.ssl.keystore.password", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_TRUSTSTORE_LOCATION)
                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.location", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.SSL_TRUSTSTORE_PASSWORD)
                .ifPresent(v -> properties.put("schema.registry.ssl.truststore.password", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE)
                .ifPresent(v -> properties.put("basic.auth.credentials.source", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BASIC_AUTH_USER_INFO)
                .ifPresent(v -> properties.put("basic.auth.user.info", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE)
                .ifPresent(v -> properties.put("bearer.auth.credentials.source", v));
        formatOptions
                .getOptional(AvroApicurioFormatOptions.BEARER_AUTH_TOKEN)
                .ifPresent(v -> properties.put("bearer.auth.token", v));

        if (properties.isEmpty()) {
            return null;
        }
        return properties;
    }

    private static Schema getAvroSchema(String schemaString, RowType rowType) {
        LogicalType convertedDataType =
                AvroSchemaConverter.convertToDataType(schemaString).getLogicalType();

        if (convertedDataType.isNullable()) {
            convertedDataType = convertedDataType.copy(false);
        }

        if (!convertedDataType.equals(rowType)) {
            throw new IllegalArgumentException(
                    format(
                            "Schema provided for '%s' format does not match the table schema: %s",
                            IDENTIFIER, schemaString));
        }

        return new Parser().parse(schemaString);
    }
}

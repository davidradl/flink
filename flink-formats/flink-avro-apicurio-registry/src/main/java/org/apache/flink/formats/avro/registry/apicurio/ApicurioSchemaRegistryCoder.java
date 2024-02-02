/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.formats.avro.SchemaCoder;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/** Reads and Writes schema using Avro Schema Registry protocol. */
public class ApicurioSchemaRegistryCoder implements SchemaCoder {

    private final RegistryClient registryClient;
    private final Map<String, Object> registryConfigs;
    private String subject;
    private static final int CONFLUENT_MAGIC_BYTE = 0;

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     * @param subject subject of schema registry to produce
     * @param registryConfigs map for registry configs
     */
    public ApicurioSchemaRegistryCoder(
            String subject, RegistryClient registryClient, Map<String, Object> registryConfigs) {
        this.registryClient = registryClient;
        this.subject = subject;
        this.registryConfigs = registryConfigs;
    }

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     */
    public ApicurioSchemaRegistryCoder(RegistryClient registryClient) {
        this.registryClient = registryClient;
        this.registryConfigs = null;
    }

    @Override
    public Schema readSchema(InputStream in) throws IOException {
        if (in == null) {
            return null;
        }

        // TODO check what is expected from the config.
        // Have an auto setting where we try each in order?
        // remember the information could be in the header
        boolean enableHeaders =
                (boolean) registryConfigs.get(AvroApicurioFormatOptions.ENABLE_HEADERS.key());
        boolean enableConfluentIdHandler =
                (boolean)
                        registryConfigs.get(
                                AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER.key());
        String idHandlerClassName =
                (String) registryConfigs.get(AvroApicurioFormatOptions.ID_HANDLER.key());
        String headersHandlerClassName =
                (String) registryConfigs.get(AvroApicurioFormatOptions.HEADERS_HANDLER.key());
        ArtifactMetaData artifactMetaData = null;

        if (enableConfluentIdHandler) {
            // extract the 4 byte magic bytes using the legacy handler
            idHandlerClassName = "io.apicurio.registry.serde.Legacy4ByteIdHandler";
        }
        if (enableHeaders) {
            // get from headers
            String groupId = "";
            String contentId = "";
            return new Schema.Parser().parse(registryClient.getLatestArtifact(groupId, contentId));
        }
        //           AvroKafkaDeserializer avroKafkaDeserializer = new
        // AvroKafkaDeserializer(registryClient);
        //           avroKafkaDeserializer.configure(registryConfigs, false);
        //           Object headerHandler = registryConfigs.get(HEADER_HANDLER);
        //
        //
        // avroKafkaDeserializer.schemaParser().parse(registryClient.getLatestArtifact(groupId, "" +
        // contentId))
        //
        //
        //
        //            io.apicurio.registry.resolver.strategy.ArtifactReference artifactReference =
        // null;
        //            if (this.headersHandler != null && headers != null) {
        //                artifactReference = this.headersHandler.readHeaders(headers);
        //                if (artifactReference.hasValue()) {
        //                    Long contentId = artifactReference.getContentId();
        //                    String groupId = artifactReference.getGroupId();
        //                    return new
        // Schema.Parser().parse(registryClient.getLatestArtifact(groupId, "" + contentId));
        //                }
        //            }

        //        } else {
        //            // the id is the in the payload
        //            // Convert kafka connection properties to a Map
        //
        //
        //            AvroKafkaDeserializer avroKafkaDeserializer = new
        // AvroKafkaDeserializer(registryClient);
        //
        //            // io.apicurio.registry.serde.DefaultIdHandler: Stores the ID as an 8-byte
        // long
        //            // io.apicurio.registry.serde.Legacy4ByteIdHandler: Stores the ID as an 4-byte
        // integer
        //            if (idHandlerClassName != null) {
        //                try {
        //                    Class idHandlerClass = Class.forName(idHandlerClassName);
        //                    IdHandler idHandler = (IdHandler)
        // idHandlerClass.getConstructor().newInstance();
        //                    int idSize = idHandler.idSize();
        //                    byte[] byteArray = new byte[idSize];
        //                    int count = 0;
        //                    while (in.available() > 0 && count < idSize) {
        //                        byteArray[count] = (byte) in.read();
        //                        count++;
        //                    }
        //
        //                    if (idSize == 8) {
        //                        long globalId = Long.parseLong(new String(byteArray));
        //                        List<ArtifactReference> artifactReferenceList =
        // registryClient.getArtifactReferencesByGlobalId(
        //                                globalId);
        //
        //                        if (!artifactReferenceList.isEmpty()) {
        //                            String artifactId =
        // artifactReferenceList.get(0).getArtifactId();
        //                            try {
        //                                // Get the schema from apicurio and convert to an avro
        // schema
        //                                return new
        // Schema.Parser().parse(registryClient.getLatestArtifact(
        //                                        "default",
        //                                        artifactId));
        //                            } catch (Exception e) {
        //                                e.printStackTrace();
        //                            }
        //                        }
        //                    } else if (idSize == 4) {
        //
        //                    }
        //
        //
        ////                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ////
        ////                    int nRead;
        ////                    // TODO hard coding size !!! A better way?
        ////                    byte[] data = new byte[16384];
        ////                    int count = 0;
        ////                    while ((nRead = in.read(data, 0, data.length)) != -1) {
        ////                        buffer.write(data, 0, nRead);
        ////                        count++;
        ////                    }
        ////                    byte[] out = Arrays.copyOfRange(data, 1, count);
        ////                    // TODO pass through headers
        ////                    return (Schema) avroKafkaDeserializer.deserialize(this.subject, out)
        ////                } catch (ClassNotFoundException e) {
        ////                    throw new RuntimeException(
        ////                            "idHandler specifies "
        ////                                    + idHandlerClassName
        ////                                    + " but that class could not be found)");
        ////                } catch (InvocationTargetException e) {
        ////                    throw new RuntimeException(e);
        ////                } catch (InstantiationException e) {
        ////                    throw new RuntimeException(e);
        ////                } catch (IllegalAccessException e) {
        ////                    throw new RuntimeException(e);
        ////                } catch (NoSuchMethodException e) {
        ////                    throw new RuntimeException(e);
        ////                }
        ////            } else {
        ////                throw new IOException(
        ////                        "Id strategy not provided in config. Update the format's config
        // to indicate how to get the artifact id.");
        ////            }
        return null;
    }
    // TODO convert the artifact id to the schema.
    //        return null;
    //                if (dataInputStream.readLong() != 0) {
    //                    throw new IOException("Unknown data format. Global id not present");
    //                } else {
    //                    AvroKafkaDeserializer avroKafkaDeserializer = new
    //         AvroKafkaDeserializer(registryClient);
    //
    //                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    //
    //                    int nRead;
    //                    // TODO hard coding size !!! A better way?
    //                    byte[] data = new byte[16384];
    //                    int count = 0;
    //                    while ((nRead = in.read(data, 0, data.length)) != -1) {
    //                        buffer.write(data, 0, nRead);
    //                        count++;
    //                    }
    //                    byte[] out = Arrays.copyOfRange(data, 1, count);
    //                    // TODO pass through headers
    //                    return (Schema) avroKafkaDeserializer.deserialize(this.subject, out);
    //                }
    //    }

    @Override
    public void writeSchema(Schema schema, OutputStream out) throws IOException {
        boolean enableHeaders =
                (boolean) registryConfigs.get(AvroApicurioFormatOptions.ENABLE_HEADERS.key());
        boolean enableConfluentIdHandler =
                (boolean)
                        registryConfigs.get(
                                AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER.key());
        String idHandlerClassName =
                (String) registryConfigs.get(AvroApicurioFormatOptions.ID_HANDLER.key());
        String headersHandler =
                (String) registryConfigs.get(AvroApicurioFormatOptions.HEADERS_HANDLER.key());
        ArtifactMetaData artifactMetaData = null;

        if (enableConfluentIdHandler) {
            // extract the 4 byte magic bytes using the legacy handler
            idHandlerClassName = "io.apicurio.registry.serde.Legacy4ByteIdHandler";
        }
        //                try {
        //                    ArtifactMetaData metaData =
        //                            registryClient.get()
        //
        //                    out.write(metaData.getGlobalId());
        //                    byte[] schemaIdBytes =
        // ByteBuffer.allocate(4).putInt(registeredId).array();
        //                    out.write(schemaIdBytes);
        //                } catch (RestClientException e) {
        //                    throw new IOException("Could not register schema in registry", e);
        //                }
    }
}

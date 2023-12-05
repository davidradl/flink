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
import org.apache.avro.Schema;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Reads and Writes schema using Apicurio Schema Registry protocol. */
public class ApicurioSchemaRegistryCoder implements SchemaCoder {

    private final RegistryClient registryClient;
    private String subject;

    /**
     * Creates {@link SchemaCode} that uses provided {@link registryClient} to connect to the schema
     * registry.
     *
     * @param registryClient client to connect to the schema registry
     * @param subject subject of schema registry to produce
     */
    public ApicurioSchemaRegistryCoder(String subject, RegistryClient registryClient) {
        this.registryClient = registryClient;
        this.subject = subject;
    }

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     */
    public ApicurioSchemaRegistryCoder(RegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    public static final byte MAGIC_BYTE = 0x0;

    @Override
    public Schema readSchema(InputStream in) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(in);
        //        byte[] bytes = new byte[in.available()];
        /*
         * There are 3 types of event that we can accept. We are looking for the artifact id to use to look up
         * the schema. This id can be in one of 3 places
         * 1) in the message payload as a content id
         * 2) in the message payload as a global id
         * 3) in a header
         * 4) emulating confluent with a MAGIC byte at the start - ignoring this for now
         *
         * For the first attempt only support globalId, Which means that we expect a magic byte then the global id followed the message
         * https://www.apicur.io/registry/docs/apicurio-registry/2.5.x/getting-started/assembly-using-kafka-client-serdes.html#registry-serdes-concepts-strategy_registry
         *
         * The code in Apicurio (AbstractKafkaDeserializer deserialise) honours the payload content then check for header content.
         *
         */

        // Global id case
        if (dataInputStream.readByte() != MAGIC_BYTE) {
            // Error no magic byte
        } else {
            // io.apicurio.registry.serde.DefaultIdHandler: Stores the ID as an 8-byte long
            // io.apicurio.registry.serde.Legacy4ByteIdHandler: Stores the ID as an 4-byte integer
            Long globalId = dataInputStream.readLong();

            InputStream schemaInputStream = this.registryClient.getContentByGlobalId(globalId);
            Schema.Parser schemaDefinitionParser = new Schema.Parser();
            // return the writer schema
            return schemaDefinitionParser.parse(schemaInputStream);
        }
        return null;

        // Read in the stream as is to get the schema

        // get the schema as is from the payload like this
        // Schema.Parser schemaDefinitionParser = new Schema.Parser();
        // Schema readerSchema = schemaDefinitionParser.parse(in);
        //                if (dataInputStream.readByte() != MAGIC_BYTE) {
        //                   // no magic byte
        //
        //
        //                } else {
        //                    int schemaId = dataInputStream.readInt();
        //
        //                    try {
        //                       // return this.schemaRegistryClient.getById(schemaId);
        //                        GroupMetaData groupMetaData =
        //         this.registryClient.getArtifactGroupId(groupId);
        //        //                groupMetaData.
        //                        // TODO how do we return the Avro schema?
        //                        return null;
        //
        //
        //                    } catch (RestClientException e) {
        //                        throw new IOException(
        //                                format("Could not find schema with id %s in registry",
        // schemaId), e);
        //                    }
        //                }
    }

    @Override
    public void writeSchema(Schema schema, OutputStream out) throws IOException {
        //        // TODO write equivalent schema for Apicurio using group id.
        //        try {
        //            int registeredId = registryClient.register(subject, schema);
        //            out.write(MAGIC_BYTE);
        //            //                    byte[] schemaIdBytes =
        //            // ByteBuffer.allocate(4).putInt(registeredId).array();
        //            byte[] schemaIdBytes = ByteBuffer.allocate(8).putLong(registeredId).array();
        //            out.write(schemaIdBytes);
        //        } catch (RestClientException e) {
        //            throw new IOException("Could not register schema in registry", e);
        //        }
    }
}

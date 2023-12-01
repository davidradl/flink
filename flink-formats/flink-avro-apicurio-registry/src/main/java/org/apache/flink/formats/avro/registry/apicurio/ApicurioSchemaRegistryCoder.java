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

/** Reads and Writes schema using Confluent Schema Registry protocol. */
public class ApicurioSchemaRegistryCoder implements SchemaCoder {

    private final RegistryClient registryClient;
    private String subject;
    private static final int CONFLUENT_MAGIC_BYTE = 0;

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

    @Override
    public Schema readSchema(InputStream in) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(in);
        /*
         * There are 3 types of event that we can accept. We are looking for the artifact id to use to look up
         * the schema. This id can be in one of 3 places
         * 1) in the message payload as a content id
         * 2) in the message payload as a global id
         * 3) in a header
         *
         * The code in Apicurio (AbstractKafkaDeserializer deserilise) honours the payload content then check for header content.
         *
         */

        // Read in the stream as is to get the schema

        // get the schema as is from the payload like this
        // Schema.Parser schemaDefinitionParser = new Schema.Parser();
        // Schema readerSchema = schemaDefinitionParser.parse(in);

        // We need the schema from the group id - so we can look it up in the schema registry

        // if MAGIC byte
        //        if (dataInputStream.readByte() != 0) {
        //            throw new IOException("Unknown data format. Magic number does not match");
        //        } else {
        //            int schemaId = dataInputStream.readInt();
        //
        //            try {
        //               // return this.schemaRegistryClient.getById(schemaId);
        //                GroupMetaData groupMetaData =
        // this.registryClient.getArtifactGroupId(groupId);
        ////                groupMetaData.
        //                // TODO how do we return the Avro schema?
        //                return null;
        //
        //
        //            } catch (RestClientException e) {
        //                throw new IOException(
        //                        format("Could not find schema with id %s in registry", schemaId),
        // e);
        //            }
        //        }
        return null;
    }

    @Override
    public void writeSchema(Schema schema, OutputStream out) throws IOException {
        // TODO write equivalent schema for Apicurio using group id.
        //        try {
        //            int registeredId = registryClient.register(subject, schema);
        //            out.write(CONFLUENT_MAGIC_BYTE);
        //            byte[] schemaIdBytes = ByteBuffer.allocate(4).putInt(registeredId).array();
        //            out.write(schemaIdBytes);
        //        } catch (RestClientException e) {
        //            throw new IOException("Could not register schema in registry", e);
        //        }
    }
}

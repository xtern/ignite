/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * JSON deserializer for ignite binary object.
 */
public class IgniteBinaryObjectJsonDeserializer extends JsonDeserializer<BinaryObjectImpl> {
    /** Property name to set binary type name. */
    public static final String BINARY_TYPE_PROPERTY = "binaryTypeName";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     */
    public IgniteBinaryObjectJsonDeserializer(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectImpl deserialize(JsonParser parser, DeserializationContext dCtx) throws IOException {
        String type = (String)dCtx.findInjectableValue(BINARY_TYPE_PROPERTY, null, null);

        assert !ctx.marshallerContext().isSystemType(type);

        ObjectCodec mapper = parser.getCodec();
        JsonNode jsonTree = mapper.readTree(parser);

        return (BinaryObjectImpl)deserialize0(type, jsonTree, mapper);
    }

    /**
     * @param type Type name.
     * @param jsonNode JSON node.
     * @param mapper JSON object mapper.
     * @return Deserialized object.
     * @throws IOException In case of error.
     */
    private Object deserialize0(String type, JsonNode jsonNode, ObjectCodec mapper) throws IOException {
        if (ctx.marshallerContext().isSystemType(type)) {
            Class<?> cls = IgniteUtils.classForName(type, null);

            if (cls != null)
                return mapper.treeToValue(jsonNode, cls);
        }

        BinaryTypeImpl binType = (BinaryTypeImpl)ctx.cacheObjects().binary().type(type);

        JsonTreeDeserializer deserializer =
            binType != null ? new TypedDeserializer(binType, mapper) : new UntypedDeserializer(type);

        return deserializer.deserialize(jsonNode);
    }

    /** */
    private interface JsonTreeDeserializer {
        /**
         * Deserialize JSON tree.
         *
         * @param tree JSON tree node.
         * @return Binary object.
         * @throws IOException In case of error.
         */
        BinaryObject deserialize(JsonNode tree) throws IOException;
    }

    /**
     * JSON deserializer creates a new binary type using JSON data types.
     */
    private class UntypedDeserializer implements JsonTreeDeserializer {
        /** New binary type name. */
        private final String type;

        /**
         * @param type New binary type name.
         */
        public UntypedDeserializer(String type) {
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public BinaryObject deserialize(JsonNode tree) throws IOException {
            return binaryValue(type, tree);
        }

        /**
         * @param type Ignite binary type name.
         * @param tree JSON tree node.
         * @return Binary object.
         * @throws IOException In case of error.
         */
        private BinaryObject binaryValue(String type, JsonNode tree) throws IOException {
            BinaryObjectBuilder builder = ctx.cacheObjects().builder(type);

            Iterator<Map.Entry<String, JsonNode>> itr = tree.fields();

            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> entry = itr.next();

                String field = entry.getKey();
                Object val = readValue(type, field, entry.getValue());

                builder.setField(field, val);
            }

            return builder.build();
        }

        /**
         * @param type Ignite binary type name.
         * @param field Field name.
         * @param jsonNode JSON node.
         * @return value.
         * @throws IOException In case of error.
         */
        protected Object readValue(String type, String field, JsonNode jsonNode) throws IOException {
            switch (jsonNode.getNodeType()) {
                case OBJECT:
                    return binaryValue(type + "." + field, jsonNode);

                case ARRAY:
                    List<Object> list = new ArrayList<>(jsonNode.size());
                    Iterator<JsonNode> itr = jsonNode.elements();

                    while (itr.hasNext())
                        list.add(readValue(type, field, itr.next()));

                    return list;

                case BINARY:
                    return jsonNode.binaryValue();

                case BOOLEAN:
                    return jsonNode.asBoolean();

                case NUMBER:
                    return jsonNode.numberValue();

                case STRING:
                    return jsonNode.asText();

                default:
                    return null;
            }
        }
    }

    /**
     * JSON deserializer using the Ignite binary type.
     */
    private class TypedDeserializer extends UntypedDeserializer {
        /** JSON object mapper. */
        private final ObjectCodec mapper;

        /** Binary type. */
        private final BinaryTypeImpl binType;

        /**
         * @param binType Binary type.
         * @param mapper JSON object mapper.
         */
        public TypedDeserializer(BinaryTypeImpl binType, ObjectCodec mapper) {
            super(binType.typeName());

            this.mapper = mapper;
            this.binType = binType;
        }

        /** {@inheritDoc} */
        @Override public BinaryObject deserialize(JsonNode tree) throws IOException {
            BinaryObjectBuilder builder = ctx.cacheObjects().builder(binType.typeName());
            Map<String, BinaryFieldMetadata> metas = binType.metadata().fieldsMap();

            Iterator<Map.Entry<String, JsonNode>> itr = tree.fields();

            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> entry = itr.next();

                String field = entry.getKey();
                JsonNode node = tree.get(field);
                BinaryFieldMetadata meta = metas.get(field);

                Object val = meta != null ?
                    readValue(meta.typeId(), field, node, binType) : readValue(binType.typeName(), field, node);

                builder.setField(field, val);
            }

            return builder.build();
        }

        /**
         * Extract and cast JSON node value into required object format.
         *
         * @param type Field type.
         * @param field Field name.
         * @param node JSON node.
         * @param parentType Parent type.
         * @return Extracted value.
         * @throws IOException if failed.
         */
        private Object readValue(int type, String field, JsonNode node, BinaryTypeImpl parentType) throws IOException {
            Class<?> baseCls = typeClass(type, field, node, parentType);

            if (baseCls != null)
                return mapper.treeToValue(node, baseCls);

            return deserialize0(parentType.fieldTypeName(field), node, mapper);
        }

        /**
         * @param type Field type.
         * @param field Field name.
         * @param node JSON node.
         * @param parentType Parent type.
         * @return Class.
         * @throws IOException if failed.
         */
        private Class<?> typeClass(int type, String field, JsonNode node, BinaryTypeImpl parentType) throws IOException {
            switch (type) {
                case GridBinaryMarshaller.MAP:
                    return Map.class;
                case GridBinaryMarshaller.OBJ_ARR:
                case GridBinaryMarshaller.COL:
                case GridBinaryMarshaller.OBJ:
                case GridBinaryMarshaller.ENUM:
                    Class<?> baseCls = getFieldClass(parentType, field);

                    if (baseCls == null && !node.isArray())
                        throw new IOException("Unable to deserialize field [name=" + field + ", type=" + type + "]");

                    return baseCls == null ? ArrayList.class : baseCls;
                default:
                    return BinaryUtils.FLAG_TO_CLASS.get((byte)type);
            }
        }

        /**
         * @param type Binary type.
         * @param field Field name.
         * @return Class for the specified field or {@code null} if the class was not resolved.
         */
        private @Nullable Class<?> getFieldClass(BinaryTypeImpl type, String field) {
            try {
                BinaryClassDescriptor binClsDesc =
                    type.context().descriptorForTypeId(false, type.typeId(),null,false);

                if (binClsDesc != null)
                    return binClsDesc.describedClass().getDeclaredField(field).getType();
            }
            catch (NoSuchFieldException | BinaryObjectException ignore) {
                // No-op.
            }

            return null;
        }
    }
}

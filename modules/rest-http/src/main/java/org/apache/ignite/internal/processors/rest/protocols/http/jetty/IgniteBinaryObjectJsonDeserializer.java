package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;

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
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectImpl deserialize(JsonParser parser, DeserializationContext dCtx) throws IOException {
        String type = (String)dCtx.findInjectableValue(BINARY_TYPE_PROPERTY, null, null);

        JsonNode jsonNode = parser.getCodec().readTree(parser);

        assert !ctx.marshallerContext().isSystemType(type);

        return (BinaryObjectImpl)deserialize0(type, jsonNode, parser);
    }

    /**
     * @param type Type name.
     * @param jsonNode JSON node.
     * @param parser JSON parser.
     * @return Deserialized object.
     * @throws IOException In case of error.
     */
    private Object deserialize0(String type, JsonNode jsonNode, JsonParser parser) throws IOException {
        if (ctx.marshallerContext().isSystemType(type)) {
            Class<?> cls = IgniteUtils.classForName(type, null);

            if (cls != null)
                return parser.getCodec().treeToValue(jsonNode, cls);
        }

        BinaryTypeImpl binType = (BinaryTypeImpl)ctx.cacheObjects().binary().type(type);

        JsonTreeDeserializer deserializer =
            binType != null ? new TypedDeserializer(binType, parser) : new UntypedDeserializer(type);

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
     * JSON deserializer using the Ignite binary type.
     */
    private class TypedDeserializer implements JsonTreeDeserializer {
        /** JSON parser. */
        private final JsonParser parser;

        /** Binary type. */
        private final BinaryTypeImpl binType;

        /**
         * @param binType Binary type.
         * @param parser JSON parser.
         */
        public TypedDeserializer(BinaryTypeImpl binType, JsonParser parser) {
            assert binType != null;

            this.parser = parser;
            this.binType = binType;
        }

        /** {@inheritDoc} */
        @Override public BinaryObject deserialize(JsonNode tree) throws IOException {
            BinaryObjectBuilder builder = ctx.cacheObjects().builder(binType.typeName());

            // todo json fields length > metadata length
            for (Map.Entry<String, BinaryFieldMetadata> e : binType.metadata().fieldsMap().entrySet()) {
                String field = e.getKey();
                JsonNode node = tree.get(field);

                if (node == null)
                    continue;

                Object val = readValue(node, field, e.getValue().typeId(), binType);

                builder.setField(field, val);
            }

            return builder.build();
        }

        /**
         * Extract and cast JSON node value into required object format.
         *
         * @param node JSON node.
         * @param field Field name.
         * @param type Field type.
         * @param parentType Parent type.
         * @return Extracted value.
         * @throws IOException if failed.
         */
        private Object readValue(JsonNode node, String field, int type, BinaryTypeImpl parentType) throws IOException {
            switch (type) {
                case GridBinaryMarshaller.BYTE:
                    return (byte)node.shortValue();

                case GridBinaryMarshaller.SHORT:
                    return node.shortValue();

                case GridBinaryMarshaller.INT:
                    return node.intValue();

                case GridBinaryMarshaller.LONG:
                    return node.longValue();

                case GridBinaryMarshaller.FLOAT:
                    return node.floatValue();

                case GridBinaryMarshaller.DOUBLE:
                    return node.doubleValue();

                case GridBinaryMarshaller.STRING:
                    return node.asText();

                case GridBinaryMarshaller.MAP:
                    return parser.getCodec().treeToValue(node, Map.class);

                case GridBinaryMarshaller.BYTE_ARR:
                    return node.binaryValue();

                case GridBinaryMarshaller.SHORT_ARR:
                    return toArray(type, node, JsonNode::shortValue);

                case GridBinaryMarshaller.INT_ARR:
                    return toArray(type, node, JsonNode::intValue);

                case GridBinaryMarshaller.LONG_ARR:
                    return toArray(type, node, JsonNode::longValue);

                case GridBinaryMarshaller.FLOAT_ARR:
                    return toArray(type, node, JsonNode::floatValue);

                case GridBinaryMarshaller.DOUBLE_ARR:
                    return toArray(type, node, JsonNode::doubleValue);

                case GridBinaryMarshaller.DECIMAL_ARR:
                    return toArray(type, node, JsonNode::decimalValue);

                case GridBinaryMarshaller.BOOLEAN_ARR:
                    return toArray(type, node, JsonNode::booleanValue);

                case GridBinaryMarshaller.CHAR_ARR:
                    return node.asText().toCharArray();

                case GridBinaryMarshaller.UUID_ARR:
                    return toArray(type, node, n -> UUID.fromString(n.asText()));

                case GridBinaryMarshaller.STRING_ARR:
                    return toArray(type, node, JsonNode::asText);

                case GridBinaryMarshaller.TIMESTAMP_ARR:
                    return toArray(type, node, n -> Timestamp.valueOf(n.textValue()));

                case GridBinaryMarshaller.DATE_ARR:
                    return toArray(type, node, n -> {
                        try {
                            return parser.getCodec().treeToValue(n, java.util.Date.class);
                        }
                        catch (IOException e) {
                            throw new IllegalArgumentException("Unable to parse date [field=" + field + "]", e);
                        }
                    });

                case GridBinaryMarshaller.OBJ:
                case GridBinaryMarshaller.OBJ_ARR:
                case GridBinaryMarshaller.COL:
                    Class<?> cls = getFieldClass(parentType, field);

                    if (node.isArray()) {
                        if (cls == null)
                            cls = ArrayList.class;

                        return parser.getCodec().treeToValue(node, cls);
                    }

                    if (cls == null)
                        throw new IOException("Unable to deserialize field [name=" + field + ", type=" + type + "]");

                    return deserialize0(cls.getName(), node, parser);
            }

            Class<?> sysCls = BinaryUtils.FLAG_TO_CLASS.get((byte)type);

            String typeName = sysCls == null ? parentType.fieldTypeName(field) : sysCls.getName();

            return deserialize0(typeName, node, parser);
        }

        private Class<?> getFieldClass(BinaryTypeImpl type, String field) {
            try {
                BinaryClassDescriptor binClsDesc =
                    type.context().descriptorForTypeId(false, type.typeId(),null,false);

                if (binClsDesc != null)
                    return binClsDesc.describedClass().getDeclaredField(field).getType();
            }
            catch (NoSuchFieldException ignore) {
                // No-op.
            }

            return null;
        }

        /**
         * Fill array using JSON node elements.
         *
         * @param typeId Binary type ID.
         * @param jsonNode JSON mpde/
         * @param mapFunc Function to extract data from JSON element.
         * @param <T> Array element type..
         * @return Resulting array.
         */
        private <T> Object toArray(int typeId, JsonNode jsonNode, Function<JsonNode, T> mapFunc) {
            Class<?> arrCls = BinaryUtils.FLAG_TO_CLASS.get((byte)typeId);

            assert arrCls != null : typeId;

            Object arr = Array.newInstance(arrCls.getComponentType(), jsonNode.size());

            for (int i = 0; i < jsonNode.size(); i++)
                Array.set(arr, i, mapFunc.apply(jsonNode.get(i)));

            return arr;
        }
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
        private Object readValue(String type, String field, JsonNode jsonNode) throws IOException {
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
}

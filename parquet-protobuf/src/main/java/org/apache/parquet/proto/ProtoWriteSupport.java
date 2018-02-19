/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.parquet.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of {@link WriteSupport} for writing Protocol Buffers.
 * @author Lukas Nalezenec
 */
public class ProtoWriteSupport<T extends MessageOrBuilder> extends WriteSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoWriteSupport.class);
  public static final String PB_CLASS_WRITE = "parquet.proto.writeClass";
  // if set to true, default value will be written to disk to ensure compatibility with other systems such as Hive,
  // Presto, Spark... If false, default value won't be written, instead fields will be set to NULL to ensure
  // backward compatibility.
  public static final String PB_WRITE_DEFAULT_VALUES = "parquet.proto.writeDefaultValues";

  private RecordConsumer recordConsumer;
  private Class<? extends Message> protoMessage;
  private MessageWriter messageWriter;
  private boolean writeDefaultValues = false;

  public ProtoWriteSupport() {
  }

  public ProtoWriteSupport(Class<? extends Message> protobufClass) {
    this.protoMessage = protobufClass;
  }

  @Override
  public String getName() {
    return "protobuf";
  }

  public static void setSchema(Configuration configuration, Class<? extends Message> protoClass) {
    configuration.setClass(PB_CLASS_WRITE, protoClass, Message.class);
  }

  /**
   * Writes Protocol buffer to parquet file.
   * @param record instance of Message.Builder or Message.
   * */
  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    try {
      messageWriter.writeTopLevelMessage(record);
    } catch (RuntimeException e) {
      Message m = (record instanceof Message.Builder) ? ((Message.Builder) record).build() : (Message) record;
      LOG.error("Cannot write message " + e.getMessage() + " : " + m);
      throw e;
    }
    recordConsumer.endMessage();
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public WriteContext init(Configuration configuration) {

    // if no protobuf descriptor was given in constructor, load descriptor from configuration (set with setProtobufClass)
    if (protoMessage == null) {
      Class<? extends Message> pbClass = configuration.getClass(PB_CLASS_WRITE, null, Message.class);
      if (pbClass != null) {
        protoMessage = pbClass;
      } else {
        String msg = "Protocol buffer class not specified.";
        String hint = " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
        throw new BadConfigurationException(msg + hint);
      }
    }

    MessageType rootSchema = new ProtoSchemaConverter().convert(protoMessage);
    Descriptor messageDescriptor = Protobufs.getMessageDescriptor(protoMessage);
    validatedMapping(messageDescriptor, rootSchema);

    this.messageWriter = new MessageWriter(messageDescriptor, rootSchema);

    // proto 3 only has optional field, and a field set to its default value is considered not set for proto3 and
    // would be written as NULL on the parquet file. However this poses compatibility issues with other systems
    // as they would have to be aware of this fact and convert NULL parquet values to the default protobuf value for
    // the type. Thus is is often desirable to write the default values instead of NULL on disk for protobuf 3.
    this.writeDefaultValues = isProto3(messageDescriptor) && configuration.getBoolean(PB_WRITE_DEFAULT_VALUES, writeDefaultValues);

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(ProtoReadSupport.PB_CLASS, protoMessage.getName());
    extraMetaData.put(ProtoReadSupport.PB_DESCRIPTOR, serializeDescriptor(protoMessage));
    return new WriteContext(rootSchema, extraMetaData);
  }

  private boolean isProto3(Descriptor messageDescriptor) {
    return messageDescriptor.getFile().getSyntax() == Descriptors.FileDescriptor.Syntax.PROTO3;
  }

  class FieldWriter {
    String fieldName;
    int index = -1;

     void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /** sets index of field inside parquet message.*/
     void setIndex(int index) {
      this.index = index;
    }

    /** Used for writing repeated fields*/
     void writeRawValue(Object value) {

    }

    /** Used for writing nonrepeated (optional, required) fields*/
    void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      writeRawValue(value);
      recordConsumer.endField(fieldName, index);
    }
  }

  class MessageWriter extends FieldWriter {

    final FieldWriter[] fieldWriters;
    private final Descriptors.FieldDescriptor[] fields;

    @SuppressWarnings("unchecked")
    MessageWriter(Descriptor descriptor, GroupType schema) {
      this.fields = (Descriptors.FieldDescriptor[]) descriptor.getFields().toArray();
      fieldWriters = (FieldWriter[]) Array.newInstance(FieldWriter.class, fields.length);

      for (FieldDescriptor fieldDescriptor: fields) {
        String name = fieldDescriptor.getName();
        Type type = schema.getType(name);
        FieldWriter writer = createWriter(fieldDescriptor, type);

        if(fieldDescriptor.isRepeated() && !fieldDescriptor.isMapField()) {
         writer = new ArrayWriter(writer);
        }

        writer.setFieldName(name);
        writer.setIndex(schema.getFieldIndex(name));

        fieldWriters[fieldDescriptor.getIndex()] = writer;
      }
    }

    private FieldWriter createWriter(FieldDescriptor fieldDescriptor, Type type) {

      switch (fieldDescriptor.getJavaType()) {
        case STRING: return new StringWriter() ;
        case MESSAGE: return createMessageWriter(fieldDescriptor, type);
        case INT: return new IntWriter();
        case LONG: return new LongWriter();
        case FLOAT: return new FloatWriter();
        case DOUBLE: return new DoubleWriter();
        case ENUM: return new EnumWriter();
        case BOOLEAN: return new BooleanWriter();
        case BYTE_STRING: return new BinaryWriter();
      }

      return unknownType(fieldDescriptor);//should not be executed, always throws exception.
    }

    private FieldWriter createMessageWriter(FieldDescriptor fieldDescriptor, Type type) {
      if (fieldDescriptor.isMapField()) {
        return createMapWriter(fieldDescriptor, type);
      }

      return new MessageWriter(fieldDescriptor.getMessageType(), getGroupType(type));
    }

    private GroupType getGroupType(Type type) {
      if (type.getOriginalType() == OriginalType.LIST) {
        return type.asGroupType().getType("list").asGroupType().getType("element").asGroupType();
      }

      if (type.getOriginalType() == OriginalType.MAP) {
        return type.asGroupType().getType("key_value").asGroupType().getType("value").asGroupType();
      }

      return type.asGroupType();
    }

    private MapWriter createMapWriter(FieldDescriptor fieldDescriptor, Type type) {
      List<FieldDescriptor> fields = fieldDescriptor.getMessageType().getFields();
      if (fields.size() != 2) {
        throw new UnsupportedOperationException("Expected two fields for the map (key/value), but got: " + fields);
      }

      // KeyFieldWriter
      FieldDescriptor keyProtoField = fields.get(0);
      FieldWriter keyWriter = createWriter(keyProtoField, type);
      keyWriter.setFieldName(keyProtoField.getName());
      keyWriter.setIndex(0);

      // ValueFieldWriter
      FieldDescriptor valueProtoField = fields.get(1);
      FieldWriter valueWriter = createWriter(valueProtoField, type);
      valueWriter.setFieldName(valueProtoField.getName());
      valueWriter.setIndex(1);

      return new MapWriter(keyWriter, valueWriter);
    }

    /** Writes top level message. It cannot call startGroup() */
    void writeTopLevelMessage(Object value) {
      writeAllFields((MessageOrBuilder) value);
    }

    /** Writes message as part of repeated field. It cannot start field*/
    @Override
    final void writeRawValue(Object value) {
      writeAllFields((MessageOrBuilder) value);
    }

    /** Used for writing nonrepeated (optional, required) fields*/
    @Override
    final void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();
      writeAllFields((MessageOrBuilder) value);
      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }

    private void writeAllFields(MessageOrBuilder pb) {
      //returns changed fields with values. Map is ordered by id.
      Map<Descriptors.FieldDescriptor, Object> fields =
        (writeDefaultValues) ? getAllFieldsIncludingDefault(pb) : getChangedFields(pb);

      for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : fields.entrySet()) {
        FieldDescriptor fieldDescriptor = entry.getKey();

        if(fieldDescriptor.isExtension()) {
          // Field index of an extension field might overlap with a base field.
          throw new UnsupportedOperationException(
                  "Cannot convert Protobuf message with extension field(s)");
        }

        int fieldIndex = fieldDescriptor.getIndex();
        fieldWriters[fieldIndex].writeField(entry.getValue());
      }
    }

    private Map<Descriptors.FieldDescriptor, Object> getAllFieldsIncludingDefault(MessageOrBuilder pb) {
      // we need a TreeMap to ensure that fields will be written in order, according to their index
      Map<Descriptors.FieldDescriptor, Object> res = new TreeMap<>(FieldDescriptorComparator);
      for (Descriptors.FieldDescriptor field: fields) {
        // getField(myField) would return the default value on a MESSAGE type when we want a null value (as calling
        // getMyField() on the protobuf message would return)
        if (!field.isExtension() && !isEmptyOptionalMessage(pb, field) && !isUnsetOneof(pb, field)) {
          res.put(field, pb.getField(field));
        }
      }
      return res;
    }

    private boolean isEmptyOptionalMessage(MessageOrBuilder pb, Descriptors.FieldDescriptor fieldDescriptor) {
      return fieldDescriptor.isOptional()
        && fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
        && !pb.hasField(fieldDescriptor);
    }

    private boolean isUnsetOneof(MessageOrBuilder pb, Descriptors.FieldDescriptor fieldDescriptor) {
      return fieldDescriptor.getContainingOneof() != null &&
        !fieldDescriptor.equals(pb.getOneofFieldDescriptor(fieldDescriptor.getContainingOneof()));
    }

    private Map<Descriptors.FieldDescriptor, Object> getChangedFields(MessageOrBuilder pb) {
      return pb.getAllFields();
    }

    // We can not use JAVA 8 lambda with method reference here as it makes the maven-shade plugin crash
    // with an ArrayOutOfBound exception.
    private Comparator<Descriptors.FieldDescriptor> FieldDescriptorComparator = new Comparator<FieldDescriptor>() {
      @Override
      public int compare(FieldDescriptor o1, FieldDescriptor o2) {
        return o1.getIndex() - o2.getIndex();
      }
    };
  }

  class ArrayWriter extends FieldWriter {
    final FieldWriter fieldWriter;

    ArrayWriter(FieldWriter fieldWriter) {
      this.fieldWriter = fieldWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      throw new UnsupportedOperationException("Array has no raw value");
    }

    @Override
    final void writeField(Object value) {
      recordConsumer.startField(fieldName, index);
      recordConsumer.startGroup();
      List<?> list = (List<?>) value;

      if (!list.isEmpty()) {
        // skip inner array field if list is empty
        recordConsumer.startField("list", 0); // This is the wrapper group for the array field

        for (Object listEntry: list) {
          recordConsumer.startGroup();
          recordConsumer.startField("element", 0); // This is the mandatory inner field

          if (!isPrimitive(listEntry)) {
            recordConsumer.startGroup();
            fieldWriter.writeRawValue(listEntry);
            recordConsumer.endGroup();
          }
          else {
            fieldWriter.writeRawValue(listEntry);
          }

          recordConsumer.endField("element", 0);
          recordConsumer.endGroup();
        }
        recordConsumer.endField("list", 0);
      }

      recordConsumer.endGroup();
      recordConsumer.endField(fieldName, index);
    }
  }

  private boolean isPrimitive(Object listEntry) {
    return !(listEntry instanceof Message);
  }

  /** validates mapping between protobuffer fields and parquet fields.*/
  private void validatedMapping(Descriptors.Descriptor descriptor, GroupType parquetSchema) {
    List<Descriptors.FieldDescriptor> allFields = descriptor.getFields();

    for (Descriptors.FieldDescriptor fieldDescriptor: allFields) {
      String fieldName = fieldDescriptor.getName();
      int fieldIndex = fieldDescriptor.getIndex();
      int parquetIndex = parquetSchema.getFieldIndex(fieldName);
      if (fieldIndex != parquetIndex) {
        String message = "FieldIndex mismatch name=" + fieldName + ": " + fieldIndex + " != " + parquetIndex;
        throw new IncompatibleSchemaModificationException(message);
      }
    }
  }


  class StringWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      Binary binaryString = Binary.fromString((String) value);
      recordConsumer.addBinary(binaryString);
    }
  }

  class IntWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addInteger((Integer) value);
    }
  }

  class LongWriter extends FieldWriter {

    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addLong((Long) value);
    }
  }

  class MapWriter extends FieldWriter {

    private final FieldWriter keyWriter;
    private final FieldWriter valueWriter;

    public MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
      super();
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    final void writeRawValue(Object value) {
      recordConsumer.startGroup();

      Collection<Message> collection = (Collection<Message>) value;

      if (!collection.isEmpty()) {
        // skip inner field all together if map is empty

        recordConsumer.startField("key_value", 0); // This is the wrapper group for the map field
        for (Message msg : collection) {
          recordConsumer.startGroup();

          final Descriptor descriptorForType = msg.getDescriptorForType();
          final FieldDescriptor keyDesc = descriptorForType.findFieldByName("key");
          final FieldDescriptor valueDesc = descriptorForType.findFieldByName("value");

          keyWriter.writeField(msg.getField(keyDesc));
          valueWriter.writeField(msg.getField(valueDesc));

          recordConsumer.endGroup();
        }

        recordConsumer.endField("key_value", 0);
      }

      recordConsumer.endGroup();
    }
  }

  class FloatWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addFloat((Float) value);
    }
  }

  class DoubleWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addDouble((Double) value);
    }
  }

  class EnumWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      Binary binary = Binary.fromString(((Descriptors.EnumValueDescriptor) value).getName());
      recordConsumer.addBinary(binary);
    }
  }

  class BooleanWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      recordConsumer.addBoolean((Boolean) value);
    }
  }

  class BinaryWriter extends FieldWriter {
    @Override
    final void writeRawValue(Object value) {
      ByteString byteString = (ByteString) value;
      Binary binary = Binary.fromConstantByteArray(byteString.toByteArray());
      recordConsumer.addBinary(binary);
    }
  }

  private FieldWriter unknownType(FieldDescriptor fieldDescriptor) {
    String exceptionMsg = "Unknown type with descriptor \"" + fieldDescriptor
            + "\" and type \"" + fieldDescriptor.getJavaType() + "\".";
    throw new InvalidRecordException(exceptionMsg);
  }

  /** Returns message descriptor as JSON String*/
  private String serializeDescriptor(Class<? extends Message> protoClass) {
    Descriptor descriptor = Protobufs.getMessageDescriptor(protoClass);
    DescriptorProtos.DescriptorProto asProto = descriptor.toProto();
    return TextFormat.printToString(asProto);
  }

}

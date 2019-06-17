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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.Builder;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.parquet.schema.OriginalType.ENUM;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Converts a Protocol Buffer Descriptor into a Parquet schema.
 */
public class ProtoSchemaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoSchemaConverter.class);

  public MessageType convert(Class<? extends Message> protobufClass) {
    LOG.debug("Converting protocol buffer class \"" + protobufClass + "\" to parquet schema.");
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
    MessageType messageType =
        convertFields(Types.buildMessage(), descriptor.getFields())
        .named(descriptor.getFullName());
    LOG.debug("Converter info:\n " + descriptor.toProto() + " was converted to \n" + messageType);
    return messageType;
  }

  /* Iterates over list of fields. **/
  private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, List<Descriptors.FieldDescriptor> fieldDescriptors) {
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      groupBuilder =
          addField(fieldDescriptor, groupBuilder)
          .id(fieldDescriptor.getNumber())
          .named(fieldDescriptor.getName());
    }
    return groupBuilder;
  }

  private Type.Repetition getRepetition(Descriptors.FieldDescriptor descriptor) {
    if (descriptor.isRequired()) {
      return Type.Repetition.REQUIRED;
    } else if (descriptor.isRepeated()) {
      return Type.Repetition.REPEATED;
    } else {
      return Type.Repetition.OPTIONAL;
    }
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(Descriptors.FieldDescriptor descriptor, final GroupBuilder<T> builder) {
    if (descriptor.getJavaType() == JavaType.MESSAGE) {
      return addMessageField(descriptor, builder);
    }

    ParquetType parquetType = getParquetType(descriptor);
    if (descriptor.isRepeated()) {
      return addRepeatedPrimitive(descriptor, parquetType.primitiveType, parquetType.originalType, builder);
    }

    return builder.primitive(parquetType.primitiveType, getRepetition(descriptor)).as(parquetType.originalType);
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addRepeatedPrimitive(Descriptors.FieldDescriptor descriptor,
                                                                                                   PrimitiveTypeName primitiveType,
                                                                                                   OriginalType originalType,
                                                                                                   final GroupBuilder<T> builder) {
    return builder
        .group(Type.Repetition.REQUIRED).as(OriginalType.LIST)
          .group(Type.Repetition.REPEATED)
            .primitive(primitiveType, Type.Repetition.REQUIRED).as(originalType)
          .named("element")
        .named("list");
  }

  private <T> GroupBuilder<GroupBuilder<T>> addRepeatedMessage(Descriptors.FieldDescriptor descriptor, GroupBuilder<T> builder) {
    GroupBuilder<GroupBuilder<GroupBuilder<T>>> result =
      builder
        .group(Type.Repetition.REQUIRED).as(OriginalType.LIST)
        .group(Type.Repetition.REPEATED);

    convertFields(result, descriptor.getMessageType().getFields());

    return result.named("list");
  }

  private <T> GroupBuilder<GroupBuilder<T>> addMessageField(Descriptors.FieldDescriptor descriptor, final GroupBuilder<T> builder) {
    if (descriptor.isMapField()) {
      return addMapField(descriptor, builder);
    } else if (descriptor.isRepeated()) {
      return addRepeatedMessage(descriptor, builder);
    }

    // Plain message
    GroupBuilder<GroupBuilder<T>> group = builder.group(getRepetition(descriptor));
    convertFields(group, descriptor.getMessageType().getFields());
    return group;
  }

  private <T> GroupBuilder<GroupBuilder<T>> addMapField(Descriptors.FieldDescriptor descriptor, final GroupBuilder<T> builder) {
    List<Descriptors.FieldDescriptor> fields = descriptor.getMessageType().getFields();
    if (fields.size() != 2) {
      throw new UnsupportedOperationException("Expected two fields for the map (key/value), but got: " + fields);
    }

    ParquetType mapKeyParquetType = getParquetType(fields.get(0));

    GroupBuilder<GroupBuilder<GroupBuilder<T>>> group = builder
      .group(Type.Repetition.REQUIRED).as(OriginalType.MAP)
      .group(Type.Repetition.REPEATED) // key_value wrapper
      .primitive(mapKeyParquetType.primitiveType, Type.Repetition.REQUIRED).as(mapKeyParquetType.originalType).named("key");

    return addField(fields.get(1), group).named("value")
      .named("key_value");
  }

  private ParquetType getParquetType(Descriptors.FieldDescriptor fieldDescriptor) {

    JavaType javaType = fieldDescriptor.getJavaType();
    switch (javaType) {
      case INT: return ParquetType.of(INT32);
      case LONG: return ParquetType.of(INT64);
      case DOUBLE: return ParquetType.of(DOUBLE);
      case BOOLEAN: return ParquetType.of(BOOLEAN);
      case FLOAT: return ParquetType.of(FLOAT);
      case STRING: return ParquetType.of(BINARY, UTF8);
      case ENUM: return ParquetType.of(BINARY, ENUM);
      case BYTE_STRING: return ParquetType.of(BINARY);
      default:
        throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType);
    }
  }

  private static class ParquetType {
    PrimitiveTypeName primitiveType;
    OriginalType originalType;

    private ParquetType(PrimitiveTypeName primitiveType, OriginalType originalType) {
      this.primitiveType = primitiveType;
      this.originalType = originalType;
    }

    public static ParquetType of(PrimitiveTypeName primitiveType, OriginalType originalType) {
      return new ParquetType(primitiveType, originalType);
    }

    public static ParquetType of(PrimitiveTypeName primitiveType) {
      return of(primitiveType, null);
    }
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bocom.rdss.spark.sdp3x.api;

import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements {@link DatasetDefinition} as an immutable value object with fluent helper methods.
 */
public final class ImmutableDatasetDefinition implements DatasetDefinition {
  private final String name;
  private final DatasetKind kind;
  private final String comment;
  private final String format;
  private final StructType schema;
  private final Map<String, String> properties;

  /**
   * Creates one immutable dataset definition instance.
   */
  private ImmutableDatasetDefinition(
      String name,
      DatasetKind kind,
      String comment,
      String format,
      StructType schema,
      Map<String, String> properties) {
    this.name = Objects.requireNonNull(name, "name");
    this.kind = Objects.requireNonNull(kind, "kind");
    this.comment = comment;
    this.format = format;
    this.schema = schema;
    this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }

  /**
   * Creates a persisted table dataset definition.
   */
  public static ImmutableDatasetDefinition table(String name) {
    return new ImmutableDatasetDefinition(name, DatasetKind.TABLE, null, null, null,
      Collections.<String, String>emptyMap());
  }

  /**
   * Creates a materialized view dataset definition.
   */
  public static ImmutableDatasetDefinition materializedView(String name) {
    return new ImmutableDatasetDefinition(name, DatasetKind.MATERIALIZED_VIEW, null, null, null,
      Collections.<String, String>emptyMap());
  }

  /**
   * Creates a temporary view dataset definition.
   */
  public static ImmutableDatasetDefinition temporaryView(String name) {
    return new ImmutableDatasetDefinition(name, DatasetKind.TEMPORARY_VIEW, null, null, null,
      Collections.<String, String>emptyMap());
  }

  /** Returns a copy with a new comment value. */
  public ImmutableDatasetDefinition withComment(String newComment) {
    return new ImmutableDatasetDefinition(name, kind, newComment, format, schema, properties);
  }

  /** Returns a copy with a new storage format value. */
  public ImmutableDatasetDefinition withFormat(String newFormat) {
    return new ImmutableDatasetDefinition(name, kind, comment, newFormat, schema, properties);
  }

  /** Returns a copy with a new declared schema. */
  public ImmutableDatasetDefinition withSchema(StructType newSchema) {
    return new ImmutableDatasetDefinition(name, kind, comment, format, newSchema, properties);
  }

  /** Returns a copy with one additional dataset property. */
  public ImmutableDatasetDefinition withProperty(String key, String value) {
    LinkedHashMap<String, String> updated = new LinkedHashMap<>(properties);
    updated.put(key, value);
    return new ImmutableDatasetDefinition(name, kind, comment, format, schema, updated);
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public DatasetKind kind() {
    return kind;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> comment() {
    return Optional.ofNullable(comment);
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> format() {
    return Optional.ofNullable(format);
  }

  /** {@inheritDoc} */
  @Override
  public Optional<StructType> schema() {
    return Optional.ofNullable(schema);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return properties;
  }
}

/*
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.mysql.v1;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jooq.DSLContext;

import static java.nio.charset.StandardCharsets.UTF_8;
import static zipkin2.storage.mysql.v1.internal.generated.tables.ZipkinAnnotations.ZIPKIN_ANNOTATIONS;

final class SelectAutocompleteValues implements Function<DSLContext, List<String>> {
  final Schema schema;
  final String autocompleteKey;

  public SelectAutocompleteValues(Schema schema, String autocompleteKey) {
    this.schema = schema;
    this.autocompleteKey = autocompleteKey;
  }

  @Override public List<String> apply(DSLContext context) {
    return context
      .selectDistinct(ZIPKIN_ANNOTATIONS.A_VALUE)
      .from(ZIPKIN_ANNOTATIONS)
      .where(ZIPKIN_ANNOTATIONS.A_KEY.eq(autocompleteKey))
      .fetch(ZIPKIN_ANNOTATIONS.A_VALUE)
      .stream()
      .map(v -> new String(v, UTF_8))
      .collect(Collectors.toList());
  }

}

/**
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
package org.apache.hadoop.yarn.api.records.timelineservice.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * We have defined a dedicated Writer for TimelineEntity,
 * aimed at adapting to the Jersey2 framework to ensure
 * that TimelineEntity can be converted into JSON format.
 */
@Provider
@Consumes(MediaType.APPLICATION_JSON)
public class TimelineEntitySetWriter implements MessageBodyWriter<Set<TimelineEntity>> {

  private ObjectMapper objectMapper = new ObjectMapper();
  private String timelineEntityType =
      "java.util.Set<org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity>";

  @Override
  public boolean isWriteable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return timelineEntityType.equals(genericType.getTypeName());
  }

  @Override
  public void writeTo(Set<TimelineEntity> timelinePutResponse, Class<?> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
      throws IOException, WebApplicationException {
    String entity = objectMapper.writeValueAsString(timelinePutResponse);
    entityStream.write(entity.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public long getSize(Set<TimelineEntity> timelineEntities, Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return -1L;
  }
}

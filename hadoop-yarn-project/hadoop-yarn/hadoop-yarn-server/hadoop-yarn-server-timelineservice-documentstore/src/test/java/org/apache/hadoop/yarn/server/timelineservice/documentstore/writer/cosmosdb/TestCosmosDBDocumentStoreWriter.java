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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.writer.cosmosdb;

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreTestUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.DocumentStoreUtils;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.CollectionType;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity.TimelineEntityDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockStatic;

/**
 * Test case for {@link CosmosDBDocumentStoreWriter}.
 */
@ExtendWith(MockitoExtension.class)
public class TestCosmosDBDocumentStoreWriter {

  @BeforeEach
  public void setUp() {
    AsyncDocumentClient asyncDocumentClient =
        Mockito.mock(AsyncDocumentClient.class);
    Configuration conf = Mockito.mock(Configuration.class);
    mockStatic(DocumentStoreUtils.class);
    when(DocumentStoreUtils.getCosmosDBDatabaseName(conf)).
        thenReturn("FooBar");
    when(DocumentStoreUtils.createCosmosDBAsyncClient(conf)).
        thenReturn(asyncDocumentClient);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void applyingUpdatesOnPrevDocTest() throws IOException {
    MockedCosmosDBDocumentStoreWriter documentStoreWriter =
        new MockedCosmosDBDocumentStoreWriter(null);

    TimelineEntityDocument actualEntityDoc =
        new TimelineEntityDocument();
    TimelineEntityDocument expectedEntityDoc =
        DocumentStoreTestUtils.bakeTimelineEntityDoc();

    assertEquals(1, actualEntityDoc.getInfo().size());
    assertEquals(0, actualEntityDoc.getMetrics().size());
    assertEquals(0, actualEntityDoc.getEvents().size());
    assertEquals(0, actualEntityDoc.getConfigs().size());
    assertEquals(0,
        actualEntityDoc.getIsRelatedToEntities().size());
    assertEquals(0, actualEntityDoc.
        getRelatesToEntities().size());

    actualEntityDoc = (TimelineEntityDocument) documentStoreWriter
        .applyUpdatesOnPrevDoc(CollectionType.ENTITY,
            actualEntityDoc, null);

    assertEquals(expectedEntityDoc.getInfo().size(),
        actualEntityDoc.getInfo().size());
    assertEquals(expectedEntityDoc.getMetrics().size(),
        actualEntityDoc.getMetrics().size());
    assertEquals(expectedEntityDoc.getEvents().size(),
        actualEntityDoc.getEvents().size());
    assertEquals(expectedEntityDoc.getConfigs().size(),
        actualEntityDoc.getConfigs().size());
    assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getIsRelatedToEntities().size());
    assertEquals(expectedEntityDoc.getRelatesToEntities().size(),
        actualEntityDoc.getRelatesToEntities().size());
  }
}
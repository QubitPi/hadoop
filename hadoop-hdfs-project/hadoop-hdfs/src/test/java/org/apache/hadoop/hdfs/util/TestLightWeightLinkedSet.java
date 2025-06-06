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
package org.apache.hadoop.hdfs.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestLightWeightLinkedSet {

  private static final Logger LOG = LoggerFactory
      .getLogger("org.apache.hadoop.hdfs.TestLightWeightLinkedSet");
  private final ArrayList<Integer> list = new ArrayList<Integer>();
  private final int NUM = 100;
  private LightWeightLinkedSet<Integer> set;
  private Random rand;

  @BeforeEach
  public void setUp() {
    float maxF = LightWeightLinkedSet.DEFAULT_MAX_LOAD_FACTOR;
    float minF = LightWeightLinkedSet.DEFAUT_MIN_LOAD_FACTOR;
    int initCapacity = LightWeightLinkedSet.MINIMUM_CAPACITY;
    rand = new Random(Time.now());
    list.clear();
    for (int i = 0; i < NUM; i++) {
      list.add(rand.nextInt());
    }
    set = new LightWeightLinkedSet<Integer>(initCapacity, maxF, minF);
  }

  @Test
  public void testEmptyBasic() {
    LOG.info("Test empty basic");
    Iterator<Integer> iter = set.iterator();
    // iterator should not have next
    assertFalse(iter.hasNext());
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());

    // poll should return nothing
    assertNull(set.pollFirst());
    assertEquals(0, set.pollAll().size());
    assertEquals(0, set.pollN(10).size());

    LOG.info("Test empty - DONE");
  }

  @Test
  public void testOneElementBasic() {
    LOG.info("Test one element basic");
    set.add(list.get(0));
    // set should be non-empty
    assertEquals(1, set.size());
    assertFalse(set.isEmpty());

    // iterator should have next
    Iterator<Integer> iter = set.iterator();
    assertTrue(iter.hasNext());

    // iterator should not have next
    assertEquals(list.get(0), iter.next());
    assertFalse(iter.hasNext());
    LOG.info("Test one element basic - DONE");
  }

  @Test
  public void testMultiBasic() {
    LOG.info("Test multi element basic");
    // add once
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    assertEquals(list.size(), set.size());

    // check if the elements are in the set
    for (Integer i : list) {
      assertTrue(set.contains(i));
    }

    // add again - should return false each time
    for (Integer i : list) {
      assertFalse(set.add(i));
    }

    // check again if the elements are there
    for (Integer i : list) {
      assertTrue(set.contains(i));
    }

    Iterator<Integer> iter = set.iterator();
    int num = 0;
    while (iter.hasNext()) {
      assertEquals(list.get(num++), iter.next());
    }
    // check the number of element from the iterator
    assertEquals(list.size(), num);
    LOG.info("Test multi element basic - DONE");
  }

  @Test
  public void testRemoveOne() {
    LOG.info("Test remove one");
    assertTrue(set.add(list.get(0)));
    assertEquals(1, set.size());

    // remove from the head/tail
    assertTrue(set.remove(list.get(0)));
    assertEquals(0, set.size());

    // check the iterator
    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());

    // poll should return nothing
    assertNull(set.pollFirst());
    assertEquals(0, set.pollAll().size());
    assertEquals(0, set.pollN(10).size());

    // add the element back to the set
    assertTrue(set.add(list.get(0)));
    assertEquals(1, set.size());

    iter = set.iterator();
    assertTrue(iter.hasNext());
    LOG.info("Test remove one - DONE");
  }

  @Test
  public void testRemoveMulti() {
    LOG.info("Test remove multi");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    for (int i = 0; i < NUM / 2; i++) {
      assertTrue(set.remove(list.get(i)));
    }

    // the deleted elements should not be there
    for (int i = 0; i < NUM / 2; i++) {
      assertFalse(set.contains(list.get(i)));
    }

    // the rest should be there
    for (int i = NUM / 2; i < NUM; i++) {
      assertTrue(set.contains(list.get(i)));
    }

    Iterator<Integer> iter = set.iterator();
    // the remaining elements should be in order
    int num = NUM / 2;
    while (iter.hasNext()) {
      assertEquals(list.get(num++), iter.next());
    }
    assertEquals(num, NUM);
    LOG.info("Test remove multi - DONE");
  }

  @Test
  public void testRemoveAll() {
    LOG.info("Test remove all");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    for (int i = 0; i < NUM; i++) {
      assertTrue(set.remove(list.get(i)));
    }
    // the deleted elements should not be there
    for (int i = 0; i < NUM; i++) {
      assertFalse(set.contains(list.get(i)));
    }

    // iterator should not have next
    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());
    assertTrue(set.isEmpty());
    LOG.info("Test remove all - DONE");
  }

  @Test
  public void testPollOneElement() {
    LOG.info("Test poll one element");
    set.add(list.get(0));
    assertEquals(list.get(0), set.pollFirst());
    assertNull(set.pollFirst());
    LOG.info("Test poll one element - DONE");
  }

  @Test
  public void testPollMulti() {
    LOG.info("Test poll multi");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    // remove half of the elements by polling
    for (int i = 0; i < NUM / 2; i++) {
      assertEquals(list.get(i), set.pollFirst());
    }
    assertEquals(NUM / 2, set.size());
    // the deleted elements should not be there
    for (int i = 0; i < NUM / 2; i++) {
      assertFalse(set.contains(list.get(i)));
    }
    // the rest should be there
    for (int i = NUM / 2; i < NUM; i++) {
      assertTrue(set.contains(list.get(i)));
    }
    Iterator<Integer> iter = set.iterator();
    // the remaining elements should be in order
    int num = NUM / 2;
    while (iter.hasNext()) {
      assertEquals(list.get(num++), iter.next());
    }
    assertEquals(num, NUM);

    // add elements back
    for (int i = 0; i < NUM / 2; i++) {
      assertTrue(set.add(list.get(i)));
    }
    // order should be switched
    assertEquals(NUM, set.size());
    for (int i = NUM / 2; i < NUM; i++) {
      assertEquals(list.get(i), set.pollFirst());
    }
    for (int i = 0; i < NUM / 2; i++) {
      assertEquals(list.get(i), set.pollFirst());
    }
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    LOG.info("Test poll multi - DONE");
  }

  @Test
  public void testPollAll() {
    LOG.info("Test poll all");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    // remove all elements by polling
    while (set.pollFirst() != null);
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());

    // the deleted elements should not be there
    for (int i = 0; i < NUM; i++) {
      assertFalse(set.contains(list.get(i)));
    }

    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());
    LOG.info("Test poll all - DONE");
  }

  @Test
  public void testPollNOne() {
    LOG.info("Test pollN one");
    set.add(list.get(0));
    List<Integer> l = set.pollN(10);
    assertEquals(1, l.size());
    assertEquals(list.get(0), l.get(0));
    LOG.info("Test pollN one - DONE");
  }

  @Test
  public void testPollNMulti() {
    LOG.info("Test pollN multi");

    // use addAll
    set.addAll(list);

    // poll existing elements
    List<Integer> l = set.pollN(10);
    assertEquals(10, l.size());

    for (int i = 0; i < 10; i++) {
      assertEquals(list.get(i), l.get(i));
    }

    // poll more elements than present
    l = set.pollN(1000);
    assertEquals(NUM - 10, l.size());

    // check the order
    for (int i = 10; i < NUM; i++) {
      assertEquals(list.get(i), l.get(i - 10));
    }
    // set is empty
    assertTrue(set.isEmpty());
    assertEquals(0, set.size());

    LOG.info("Test pollN multi - DONE");
  }

  @Test
  public void testClear() {
    LOG.info("Test clear");
    // use addAll
    set.addAll(list);
    assertEquals(NUM, set.size());
    assertFalse(set.isEmpty());

    // Advance the bookmark.
    Iterator<Integer> bkmrkIt = set.getBookmark();
    for (int i=0; i<set.size()/2+1; i++) {
      bkmrkIt.next();
    }
    assertTrue(bkmrkIt.hasNext());

    // clear the set
    set.clear();
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    bkmrkIt = set.getBookmark();
    assertFalse(bkmrkIt.hasNext());

    // poll should return an empty list
    assertEquals(0, set.pollAll().size());
    assertEquals(0, set.pollN(10).size());
    assertNull(set.pollFirst());

    // iterator should be empty
    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());

    LOG.info("Test clear - DONE");
  }

  @Test
  public void testOther() {
    LOG.info("Test other");
    assertTrue(set.addAll(list));
    // to array
    Integer[] array = set.toArray(new Integer[0]);
    assertEquals(NUM, array.length);
    for (int i = 0; i < array.length; i++) {
      assertTrue(list.contains(array[i]));
    }
    assertEquals(NUM, set.size());

    // to array
    Object[] array2 = set.toArray();
    assertEquals(NUM, array2.length);
    for (int i = 0; i < array2.length; i++) {
      assertTrue(list.contains(array2[i]));
    }
    LOG.info("Test capacity - DONE");
  }

  @Test
  @Timeout(value = 60)
  public void testGetBookmarkReturnsBookmarkIterator() {
    LOG.info("Test getBookmark returns proper iterator");
    assertTrue(set.addAll(list));

    Iterator<Integer> bookmark = set.getBookmark();
    assertEquals(bookmark.next(), list.get(0));

    final int numAdvance = list.size()/2;
    for(int i=1; i<numAdvance; i++) {
      bookmark.next();
    }

    Iterator<Integer> bookmark2 = set.getBookmark();
    assertEquals(bookmark2.next(), list.get(numAdvance));
  }

  @Test
  @Timeout(value = 60)
  public void testBookmarkAdvancesOnRemoveOfSameElement() {
    LOG.info("Test that the bookmark advances if we remove its element.");
    assertTrue(set.add(list.get(0)));
    assertTrue(set.add(list.get(1)));
    assertTrue(set.add(list.get(2)));

    Iterator<Integer> it = set.getBookmark();
    assertEquals(it.next(), list.get(0));
    set.remove(list.get(1));
    it = set.getBookmark();
    assertEquals(it.next(), list.get(2));
  }

  @Test
  @Timeout(value = 60)
  public void testBookmarkSetToHeadOnAddToEmpty() {
    LOG.info("Test bookmark is set after adding to previously empty set.");
    Iterator<Integer> it = set.getBookmark();
    assertFalse(it.hasNext());
    set.add(list.get(0));
    set.add(list.get(1));

    it = set.getBookmark();
    assertTrue(it.hasNext());
    assertEquals(it.next(), list.get(0));
    assertEquals(it.next(), list.get(1));
    assertFalse(it.hasNext());
  }

  @Test
  @Timeout(value = 60)
  public void testResetBookmarkPlacesBookmarkAtHead() {
    set.addAll(list);
    Iterator<Integer> it = set.getBookmark();
    final int numAdvance = set.size()/2;
    for (int i=0; i<numAdvance; i++) {
      it.next();
    }
    assertEquals(it.next(), list.get(numAdvance));

    set.resetBookmark();
    it = set.getBookmark();
    assertEquals(it.next(), list.get(0));
  }
}

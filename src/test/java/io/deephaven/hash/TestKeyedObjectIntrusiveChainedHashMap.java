/*
 Copyright (C) 2021 Deephaven Data Labs (https://deephaven.io).

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Lesser General Public License as published by the Free Software Foundation, either version 3
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU Lesser General Public License for more details.
*/
package io.deephaven.hash;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/** The test case. */
public class TestKeyedObjectIntrusiveChainedHashMap
    extends AbstractTestGenericMap<String, KeyedTestObject> {
  public TestKeyedObjectIntrusiveChainedHashMap(String name) {
    super(name, 100);
  }

  private static Random random = new Random(101763);

  public HashMap<String, KeyedTestObject> generateUniqueRandomHashMap(
      int size, int min_key, int max_key) {
    HashMap<String, KeyedTestObject> m = new HashMap<String, KeyedTestObject>(size);
    assert min_key < max_key;
    assert max_key - min_key > size;
    StringBuilder sb = new StringBuilder();
    while (m.size() != size) {
      sb.setLength(0);
      int n = random.nextInt(max_key - min_key) - max_key;
      sb.append(n);
      String key = sb.toString();
      if (!m.containsKey(key)) {
        m.put(key, new KeyedTestObject(key));
      }
    }
    return m;
  }

  protected Map<String, KeyedTestObject> newTestMap(
      int initialSize, Map<String, KeyedTestObject> from) {
    KeyedTestObjectIntrusiveChainedMap map = new KeyedTestObjectIntrusiveChainedMap(initialSize);
    if (from != null) {
      for (KeyedTestObject o : from.values()) {
        map.put(o.getId(), o);
      }
    }
    return map;
  }

  protected void compact(Map map) {
    // TODO: not supported in IntrusiveChainedMaps
  }

  protected KeyedTestObject[] newValueArray(int n) {
    return new KeyedTestObject[n];
  }

  protected String[] newKeyArray(int n) {
    return new String[n];
  }

  protected KeyedTestObject newValue(String key) {
    return new KeyedTestObject(key);
  }

  protected String getKey(KeyedTestObject value) {
    return value.getId();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * If the test subject is an indexable map, make sure the getByIndex method returns identical
   * objects
   */
  protected <K, V> void assertConsistency(Map<K, V> subject) {
    if (subject instanceof IndexableMap) {
      assert subject instanceof KeyedTestObjectIntrusiveChainedMap;
      IndexableMap<String, KeyedTestObject> imap = (IndexableMap<String, KeyedTestObject>) subject;
      for (int i = 0; i < imap.size(); ++i) {
        KeyedTestObject o = imap.getByIndex(i);
        assertTrue("values are identical", o == subject.get(o.getId()));
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------

  private <K> void assertContainsKeys(Map<K, ?> map, K... keys) {
    for (K k : keys) {
      assertTrue(map.containsKey(k));
    }
  }

  public void testDebug() {
    // one bucket - makes it easy to exercise all of the linked-list manipulations
    KeyedTestObjectIntrusiveChainedMap m = new KeyedTestObjectIntrusiveChainedMap(2, 0.5F, false);
    KeyedTestObject prev;

    KeyedTestObject a = new KeyedTestObject("A");
    KeyedTestObject b = new KeyedTestObject("B");
    KeyedTestObject c = new KeyedTestObject("C");
    KeyedTestObject d = new KeyedTestObject("D");

    prev = m.add(a);
    assertEquals(1, m.size());
    assertNull(prev);
    prev = m.add(b);
    assertEquals(2, m.size());
    assertNull(prev);
    prev = m.add(c);
    assertEquals(3, m.size());
    assertNull(prev);
    prev = m.add(d);
    assertEquals(4, m.size());
    assertNull(prev);

    assertSame(a, m.get("A"));
    assertSame(b, m.get("B"));
    assertSame(c, m.get("C"));
    assertSame(d, m.get("D"));

    // replace all elements

    KeyedTestObject a2 = new KeyedTestObject("A");
    KeyedTestObject b2 = new KeyedTestObject("B");
    KeyedTestObject c2 = new KeyedTestObject("C");
    KeyedTestObject d2 = new KeyedTestObject("D");

    prev = m.add(a2);
    assertEquals(4, m.size());
    assertSame(a, prev);
    prev = m.add(b2);
    assertEquals(4, m.size());
    assertSame(b, prev);
    prev = m.add(c2);
    assertEquals(4, m.size());
    assertSame(c, prev);
    prev = m.add(d2);
    assertEquals(4, m.size());
    assertSame(d, prev);

    assertSame(a2, m.get("A"));
    assertSame(b2, m.get("B"));
    assertSame(c2, m.get("C"));
    assertSame(d2, m.get("D"));

    // addIfAbsent on all elements

    KeyedTestObject a3 = new KeyedTestObject("A");
    KeyedTestObject b3 = new KeyedTestObject("B");
    KeyedTestObject c3 = new KeyedTestObject("C");
    KeyedTestObject d3 = new KeyedTestObject("D");

    prev = m.addIfAbsent(a3);
    assertEquals(4, m.size());
    assertSame(a2, prev);
    prev = m.addIfAbsent(b3);
    assertEquals(4, m.size());
    assertSame(b2, prev);
    prev = m.addIfAbsent(c3);
    assertEquals(4, m.size());
    assertSame(c2, prev);
    prev = m.addIfAbsent(d3);
    assertEquals(4, m.size());
    assertSame(d2, prev);

    assertSame(a2, m.get("A"));
    assertSame(b2, m.get("B"));
    assertSame(c2, m.get("C"));
    assertSame(d2, m.get("D"));

    KeyedTestObject e3 = new KeyedTestObject("E");
    prev = m.addIfAbsent(e3);
    assertEquals(5, m.size());
    assertNull(prev);

    assertSame(e3, m.get("E"));

    // remove all elements

    KeyedTestObject removed;

    // remove last element in the chain
    removed = m.removeKey("E");
    assertEquals(4, m.size());
    assertSame(e3, removed);
    assertContainsKeys(m, "A", "B", "C", "D");

    removed = m.removeKey("D");
    assertEquals(3, m.size());
    assertSame(d2, removed);
    assertContainsKeys(m, "A", "B", "C");

    // remove middle element in the chain
    removed = m.removeKey("B");
    assertEquals(2, m.size());
    assertSame(b2, removed);
    assertContainsKeys(m, "A", "C");

    // remove first element in the chain
    removed = m.removeKey("A");
    assertEquals(1, m.size());
    assertSame(a2, removed);
    assertContainsKeys(m, "C");

    // remove only element in the chain
    removed = m.removeKey("C");
    assertEquals(0, m.size());
    assertSame(c2, removed);

    // iterator
    m.add(a);
    m.add(b);
    m.add(c);
    m.add(d);

    Iterator<KeyedTestObject> i = m.iterator();
    assertSame(a, i.next());
    assertSame(b, i.next());
    assertSame(c, i.next());
    assertSame(d, i.next());

    i = m.iterator();
    assertSame(a, i.next());
    assertSame(b, i.next());
    i.remove();
    assertSame(c, i.next());
  }
}

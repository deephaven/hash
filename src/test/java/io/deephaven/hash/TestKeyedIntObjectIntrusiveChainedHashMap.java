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

public class TestKeyedIntObjectIntrusiveChainedHashMap
    extends AbstractTestGenericMap<Integer, KeyedIntTestObject> {
  public TestKeyedIntObjectIntrusiveChainedHashMap(String name) {
    super(name, 100);
  }

  private static Random random = new Random(101763);

  public HashMap<Integer, KeyedIntTestObject> generateUniqueRandomHashMap(
      int size, int min_key, int max_key) {
    HashMap<Integer, KeyedIntTestObject> m = new HashMap<>(size);
    assert min_key < max_key;
    assert max_key - min_key > size;
    while (m.size() != size) {
      int key = random.nextInt(max_key - min_key) + min_key;
      if (!m.containsKey(key)) {
        m.put(key, new KeyedIntTestObject(key));
      }
    }
    return m;
  }

  protected Map<Integer, KeyedIntTestObject> newTestMap(
      int initialSize, Map<Integer, KeyedIntTestObject> from) {
    KeyedIntTestObjectIntrusiveChainedMap map =
        new KeyedIntTestObjectIntrusiveChainedMap(initialSize);
    if (from != null) {
      for (KeyedIntTestObject o : from.values()) {
        map.put(o.getId(), o);
      }
    }
    return map;
  }

  protected void compact(Map map) {
    // TODO: not supported in IntrusiveChainedMaps
  }

  protected KeyedIntTestObject[] newValueArray(int n) {
    return new KeyedIntTestObject[n];
  }

  protected Integer[] newKeyArray(int n) {
    return new Integer[n];
  }

  protected KeyedIntTestObject newValue(Integer key) {
    return new KeyedIntTestObject(key);
  }

  protected Integer getKey(KeyedIntTestObject value) {
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
      assert subject instanceof KeyedIntTestObjectIntrusiveChainedMap;
      IndexableMap<String, KeyedIntTestObject> imap =
          (IndexableMap<String, KeyedIntTestObject>) subject;
      for (int i = 0; i < imap.size(); ++i) {
        KeyedIntTestObject o = imap.getByIndex(i);
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
    KeyedIntTestObjectIntrusiveChainedMap m =
        new KeyedIntTestObjectIntrusiveChainedMap(2, 0.5F, false);

    final int A = 101;
    final int B = 102;
    final int C = 103;
    final int D = 104;

    KeyedIntTestObject a = new KeyedIntTestObject(A);
    KeyedIntTestObject b = new KeyedIntTestObject(B);
    KeyedIntTestObject c = new KeyedIntTestObject(C);
    KeyedIntTestObject d = new KeyedIntTestObject(D);

    m.add(a);
    assertEquals(1, m.size());
    m.add(b);
    assertEquals(2, m.size());
    m.add(c);
    assertEquals(3, m.size());
    m.add(d);
    assertEquals(4, m.size());

    assertSame(a, m.get(A));
    assertSame(b, m.get(B));
    assertSame(c, m.get(C));
    assertSame(d, m.get(D));

    // replace all elements

    KeyedIntTestObject a2 = new KeyedIntTestObject(A);
    KeyedIntTestObject b2 = new KeyedIntTestObject(B);
    KeyedIntTestObject c2 = new KeyedIntTestObject(C);
    KeyedIntTestObject d2 = new KeyedIntTestObject(D);

    KeyedIntTestObject prev;

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

    assertSame(a2, m.get(A));
    assertSame(b2, m.get(B));
    assertSame(c2, m.get(C));
    assertSame(d2, m.get(D));

    // remove all elements

    KeyedIntTestObject removed;

    // remove last element in the chain
    removed = m.removeKey(D);
    assertEquals(3, m.size());
    assertSame(d2, removed);
    assertContainsKeys(m, A, B, C);

    // remove middle element in the chain
    removed = m.removeKey(B);
    assertEquals(2, m.size());
    assertSame(b2, removed);
    assertContainsKeys(m, A, C);

    // remove first element in the chain
    removed = m.removeKey(A);
    assertEquals(1, m.size());
    assertSame(a2, removed);
    assertContainsKeys(m, C);

    // remove only element in the chain
    removed = m.removeKey(C);
    assertEquals(0, m.size());
    assertSame(c2, removed);

    // iterator
    m.add(a);
    m.add(b);
    m.add(c);
    m.add(d);

    Iterator<KeyedIntTestObject> i = m.iterator();
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

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKeyedIntObjectHashMap extends AbstractTestGenericMap<Integer, KeyedIntTestObject> {
  private static Logger log = LoggerFactory.getLogger(TestKeyedIntObjectHashMap.class);

  public TestKeyedIntObjectHashMap(String name) {
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
    KeyedIntTestObjectMap map = new KeyedIntTestObjectMap(initialSize);
    if (from != null) {
      for (KeyedIntTestObject o : from.values()) {
        map.put(o.getId(), o);
      }
    }
    return map;
  }

  protected void compact(Map map) {
    assert map instanceof KeyedIntTestObjectMap;
    ((KeyedIntTestObjectMap) map).compact();
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

  /**
   * If the test subject is an indexable map, make sure the getByIndex method returns identical
   * objects
   */
  protected <K, V> void assertConsistency(Map<K, V> subject) {
    assert subject instanceof KeyedIntTestObjectMap;
    IndexableMap<Integer, KeyedIntTestObject> imap =
        (IndexableMap<Integer, KeyedIntTestObject>) subject;
    for (int i = 0; i < imap.size(); ++i) {
      KeyedIntTestObject o = imap.getByIndex(i);
      assertTrue("values are identical", o == subject.get(o.getId()));
    }
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
   * Test a matrix of (low) capacity starting conditions and a range of load factors. This test will
   * expose starting conditions that are likely to cause problems with the hash table
   * implementation.
   */
  public void testCapacityAndLoadFactor() {
    // Test some very high LF edge cases
    testCapacityAndLoadFactor(0, 0.99999999f);
    testCapacityAndLoadFactor(1, 0.99999999f);
    testCapacityAndLoadFactor(2, 0.99999999f);
    testCapacityAndLoadFactor(3, 0.99999999f);
    testCapacityAndLoadFactor(4, 0.99999999f);
    testCapacityAndLoadFactor(5, 0.99999999f);

    // Test a matrix of starting conditions.
    for (float loadFactor = 0.001f; loadFactor < 1.0f; loadFactor += 0.001f) {
      for (int capacity = 0; capacity < 100; ++capacity) {
        testCapacityAndLoadFactor(capacity, loadFactor);
      }
    }
  }

  private void testCapacityAndLoadFactor(final int capacity, final float loadFactor) {
    final KeyedIntTestObjectMap m = new KeyedIntTestObjectMap(capacity, loadFactor);

    for (int i = 0; i < capacity * 2; ++i) {
      if (m.size() >= m.capacity() - 1) {
        // remove the first key
        final KeyedIntTestObject o = m.getByIndex(0);
        m.remove(o.getId());
      }

      // add a random key
      final int key = random.nextInt(10);
      final KeyedIntTestObject o = new KeyedIntTestObject(key);
      try {
        m.add(o);
      } catch (IllegalStateException ex) {
        throw new IllegalStateException(
            "testAddRemove: capacity = " + capacity + ", loadFactor = " + loadFactor, ex);
      }
    }
  }

  /**
   * Reproducer for bug documented in DH-19237, where a normal replace() call incorrectly throws NPE
   */
  public void testDH19237() {
    final KeyedIntTestObjectMap m1 = new KeyedIntTestObjectMap();
    KeyedIntTestObject o1 = new KeyedIntTestObject(1);
    KeyedIntTestObject o2 = new KeyedIntTestObject(1);

    boolean replaced;

    replaced = m1.replace(1, o2, o1);
    assertFalse("replace() should not return true when value not found", replaced);
    assertSame(m1.get(1), null); // confirm replace failed

    m1.add(o1);
    replaced = m1.replace(1, o1, o2);
    assertTrue("replace() should return true when value found and replaced", replaced);
    assertSame(m1.get(1), o2);

    // Repeat using boxed datatype
    final KeyedIntTestObjectMap m2 = new KeyedIntTestObjectMap();
    final Integer i1 = 1;

    replaced = m2.replace(i1, o1, o2);
    assertFalse("replace() should not return true when value not found", replaced);
    assertSame(m2.get(i1), null); // confirm replace failed

    m2.add(o1);
    replaced = m2.replace(i1, o2, o1);
    assertTrue("replace() should return true when value found and replaced", replaced);
    assertSame(m2.get(i1), o1);
  }

  /*
   ** tests the unboxed putIfAbsent call
   */
  public void testSimpleUnboxedPutIfAbsent() {
    final KeyedIntTestObjectMap m = new KeyedIntTestObjectMap();

    // create two objects that are equals() but not identical
    KeyedIntTestObject o1 = new KeyedIntTestObject(42);
    KeyedIntTestObject o2 = new KeyedIntTestObject(42);

    KeyedIntTestObject result;

    result = m.putIfAbsent(o1.getId(), o1);
    assertTrue(result == null);

    result = m.putIfAbsent(o2.getId(), o2);
    assertTrue(result == o1);

    assertTrue(m.get(o2.getId()) == o1);
  }

  /*
   ** tests the unboxed replace call
   */
  public void testSimpleUnboxedReplace() {
    KeyedIntTestObject result;

    final KeyedIntTestObjectMap m = new KeyedIntTestObjectMap(10);

    final KeyedIntTestObject o1 = new KeyedIntTestObject(0);
    final KeyedIntTestObject o2 = new KeyedIntTestObject(0);
    final KeyedIntTestObject o3 = new KeyedIntTestObject(0);

    result = m.putIfAbsent(0, o1);
    assertNull(result);
    assertSame(m.get(0), o1); // strict equality test

    result = m.putIfAbsent(0, o2);
    assertNotNull(result);
    assertSame(m.get(0), o1); // strict equality test

    result = m.put(0, o2);
    assertNotNull(result);
    assertSame(result, o1); // strict equality test
    assertSame(m.get(0), o2); // strict equality test

    assertFalse(m.replace(0, new KeyedIntTestObject(10), o3));
    assertSame(m.get(0), o2); // strict equality test

    assertTrue(m.replace(0, new KeyedIntTestObject(0), o3));
    assertSame(m.get(0), o3); // strict equality test
  }

  /*
   * Reproducer for bug documented in DH-18265
   */
  public void testDH18265() {
    KeyedIntTestObject result;

    final KeyedIntTestObjectMap m = new KeyedIntTestObjectMap(10);

    // Setup the conditions for the bug to be triggered.
    final int capacity = m.capacity();

    // This will hash to 0 internally
    final KeyedIntTestObject o1 = new KeyedIntTestObject(capacity);
    result = m.putIfAbsent(capacity, o1);
    assertNull(result);
    assertSame(m.get(capacity), o1); // strict equality test

    // This will also initially hash to 0, but will be double hashed to an empty slot.
    final KeyedIntTestObject o2 = new KeyedIntTestObject(0);
    result = m.putIfAbsent(0, o2);
    assertNull(result);
    assertSame(m.get(0), o2); // strict equality test

    // Remove the first object, leaving a DELETED tombstone at the 0 slot.
    result = m.remove(capacity);
    assertNotNull(result);
    assertSame(result, o1); // strict equality test

    // This replace should fail, since we do not match old values.
    final KeyedIntTestObject o3 = new KeyedIntTestObject(10);
    final KeyedIntTestObject o4 = new KeyedIntTestObject(0);
    assertFalse(m.replace(0, o3, o4));
    assertSame(m.get(0), o2); // strict equality test

    // This replace should succeed, since we match the old value.
    assertTrue(m.replace(0, o2, o4));
    assertSame(m.get(0), o4); // strict equality test
  }

  /*
   ** tests for KeyedIntObjectMaps -- putIfAbsent(K, ValueFactory)
   */
  public class KIOMPutIfAbsent<V> extends Thread {
    public final int numRuns;
    public final HashMap<Integer, V> objects;
    public final KeyedIntObjectHash<V> map;
    public final KeyedIntObjectHash.ValueFactory<V> factory;

    public KIOMPutIfAbsent(
        int numRuns,
        HashMap<Integer, V> objects,
        KeyedIntObjectHash<V> map,
        KeyedIntObjectHash.ValueFactory<V> factory) {
      this.numRuns = numRuns;
      this.objects = objects;
      this.map = map;
      this.factory = factory;
    }

    public int numRemoves = 0;

    public void run() {
      for (int i = 0; i < numRuns; ++i) {
        for (Integer k : objects.keySet()) {
          map.putIfAbsent(k.intValue(), factory); // make sure we call the right method!
        }
      }
      for (Integer k : objects.keySet()) {
        if (random.nextDouble() < 0.4) {
          if (map.removeKey(k.intValue()) != null) { // make sure we call the right method!
            ++numRemoves;
          }
        }
        if (random.nextDouble() < 0.01) {
          compact((Map<Integer, V>) map);
        }
      }
      for (Integer k : objects.keySet()) {
        map.putIfAbsent(k.intValue(), factory); // make sure we call the right method!
      }
    }
  }

  private static class KIOMPutIfAbsentFactory<V> implements KeyedIntObjectHash.ValueFactory<V> {
    public final HashMap<Integer, V> objects;

    public KIOMPutIfAbsentFactory(HashMap<Integer, V> objects) {
      this.objects = objects;
    }

    public int numCalls = 0;

    public V newValue(Integer key) {
      ++numCalls;
      return objects.get(key);
    }

    public V newValue(int key) {
      ++numCalls;
      return objects.get(key);
    }
  }

  public void testKIOMPutIfAbsent() {
    final Map<Integer, KeyedIntTestObject> map = newTestMap(10, null);
    if (!(map instanceof KeyedIntObjectHash)) {
      return;
    }
    KeyedIntObjectHash<KeyedIntTestObject> KIOM = ((KeyedIntObjectHash<KeyedIntTestObject>) map);

    final HashMap<Integer, KeyedIntTestObject> objects =
        generateUniqueRandomHashMap(SIZE * 10, MIN_KEY, MAX_KEY);
    final KIOMPutIfAbsentFactory<KeyedIntTestObject> factory =
        new KIOMPutIfAbsentFactory<>(objects);
    final int NUM_THREADS = 5;
    final int NUM_RUNS = 100;

    ArrayList<KIOMPutIfAbsent> mutators = new ArrayList<KIOMPutIfAbsent>(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
      mutators.add(new KIOMPutIfAbsent(NUM_RUNS, objects, KIOM, factory));
    }
    for (int i = 0; i < NUM_THREADS; ++i) {
      mutators.get(i).start();
    }
    int totalRemoves = 0;
    for (int i = 0; i < NUM_THREADS; ++i) {
      while (true) {
        try {
          KIOMPutIfAbsent m = mutators.get(i);
          m.join();
          totalRemoves += m.numRemoves;
          log.info("testKIOMPutIfAbsent: mutator " + i + " had " + m.numRemoves + " removes");
          break;
        } catch (InterruptedException x) {
          // don't care
        }
      }
    }

    log.info(
        "Factory had "
            + factory.numCalls
            + " calls, objects.size() is "
            + objects.size()
            + " total successful removes was "
            + totalRemoves);
    assertMapsEqual(map, objects);
    assertConsistency(map);
    assertEquals(objects.size() + totalRemoves, factory.numCalls);
  }

  public void testKIOMConcurrentGet() {
    final Map<Integer, KeyedIntTestObject> map = newTestMap(10, null);
    if (!(map instanceof KeyedIntObjectHash)) {
      return;
    }
    final KeyedIntObjectHash<KeyedIntTestObject> SUT =
        ((KeyedIntObjectHash<KeyedIntTestObject>) map);

    final long MILLIS = 1000;

    Thread setter =
        new Thread() {
          @Override
          public void run() {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() < t0 + MILLIS) {
              for (int i = 0; i < SUT.capacity(); ++i) {
                SUT.put(i, new KeyedIntTestObject(i));
                SUT.removeKey(i);
              }
            }
          }
        };

    setter.start();

    long t0 = System.currentTimeMillis();
    try {
      while (System.currentTimeMillis() < t0 + MILLIS) {
        // doesn't matter what key we get here - just has to be
        SUT.get(1);
      }
      setter.join();
    } catch (Exception x) {
      fail("Unhandled exception: " + x);
    }
  }
}

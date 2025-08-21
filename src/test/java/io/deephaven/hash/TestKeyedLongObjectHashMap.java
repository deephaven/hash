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

/** The test case. */
public class TestKeyedLongObjectHashMap extends AbstractTestGenericMap<Long, KeyedLongTestObject> {
  private static Logger log = LoggerFactory.getLogger(TestKeyedLongObjectHashMap.class);

  public TestKeyedLongObjectHashMap(String name) {
    super(name, 100);
  }

  private static Random random = new Random(101763);

  public HashMap<Long, KeyedLongTestObject> generateUniqueRandomHashMap(
      int size, int min_key, int max_key) {
    HashMap<Long, KeyedLongTestObject> m = new HashMap<>(size);
    assert min_key < max_key;
    assert max_key - min_key > size;
    while (m.size() != size) {
      long key =
          random.nextInt(max_key - min_key)
              + min_key; // Not sure it's worth using the full domain of long
      if (!m.containsKey(key)) {
        m.put(key, new KeyedLongTestObject(key));
      }
    }
    return m;
  }

  protected Map<Long, KeyedLongTestObject> newTestMap(
      int initialSize, Map<Long, KeyedLongTestObject> from) {
    KeyedLongTestObjectMap map = new KeyedLongTestObjectMap(initialSize);
    if (from != null) {
      for (KeyedLongTestObject o : from.values()) {
        map.put(o.getId(), o);
      }
    }
    return map;
  }

  protected void compact(Map map) {
    assert map instanceof KeyedLongTestObjectMap;
    ((KeyedLongTestObjectMap) map).compact();
  }

  protected KeyedLongTestObject[] newValueArray(int n) {
    return new KeyedLongTestObject[n];
  }

  protected Long[] newKeyArray(int n) {
    return new Long[n];
  }

  protected KeyedLongTestObject newValue(Long key) {
    return new KeyedLongTestObject(key);
  }

  protected Long getKey(KeyedLongTestObject value) {
    return value.getId();
  }

  /**
   * If the test subject is an indexable map, make sure the getByIndex method returns identical
   * objects
   */
  protected <K, V> void assertConsistency(Map<K, V> subject) {
    assert subject instanceof KeyedLongTestObjectMap;
    IndexableMap<Long, KeyedLongTestObject> imap =
        (IndexableMap<Long, KeyedLongTestObject>) subject;
    for (int i = 0; i < imap.size(); ++i) {
      KeyedLongTestObject o = imap.getByIndex(i);
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
    final KeyedLongTestObjectMap m = new KeyedLongTestObjectMap(capacity, loadFactor);

    for (int i = 0; i < capacity * 2; ++i) {
      if (m.size() >= m.capacity() - 1) {
        // remove the first key
        final KeyedLongTestObject o = m.getByIndex(0);
        m.remove(o.getId());
      }

      // add a random key
      final long key = random.nextInt(10);
      final KeyedLongTestObject o = new KeyedLongTestObject(key);
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
    final KeyedLongTestObjectMap m1 = new KeyedLongTestObjectMap();
    KeyedLongTestObject o1 = new KeyedLongTestObject(1);
    KeyedLongTestObject o2 = new KeyedLongTestObject(1);

    boolean replaced;

    replaced = m1.replace(1, o2, o1);
    assertFalse("replace() should not return true when value not found", replaced);
    assertSame(m1.get(1), null); // confirm replace failed

    m1.add(o1);
    replaced = m1.replace(1, o1, o2);
    assertTrue("replace() should return true when value found and replaced", replaced);
    assertSame(m1.get(1), o2);

    // Repeat using boxed datatype
    final KeyedLongTestObjectMap m2 = new KeyedLongTestObjectMap();
    final Long l1 = 1L;

    replaced = m2.replace(l1, o1, o2);
    assertFalse("replace() should not return true when value not found", replaced);
    assertSame(m2.get(l1), null); // confirm replace failed

    m2.add(o1);
    replaced = m2.replace(l1, o2, o1);
    assertTrue("replace() should return true when value found and replaced", replaced);
    assertSame(m2.get(l1), o1);
  }

  /*
   ** tests the unboxed putIfAbsent call
   */
  public void testSimpleUnboxedPutIfAbsent() {
    final KeyedLongTestObjectMap m = new KeyedLongTestObjectMap();

    // create two objects that are equals() but not identical
    KeyedLongTestObject o1 = new KeyedLongTestObject(42);
    KeyedLongTestObject o2 = new KeyedLongTestObject(42);

    KeyedLongTestObject result;

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
    KeyedLongTestObject result;

    final KeyedLongTestObjectMap m = new KeyedLongTestObjectMap(10);

    final KeyedLongTestObject o1 = new KeyedLongTestObject(0);
    final KeyedLongTestObject o2 = new KeyedLongTestObject(0);
    final KeyedLongTestObject o3 = new KeyedLongTestObject(0);

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

    assertFalse(m.replace(0, new KeyedLongTestObject(10), o3));
    assertSame(m.get(0), o2); // strict equality test

    assertTrue(m.replace(0, new KeyedLongTestObject(0), o3));
    assertSame(m.get(0), o3); // strict equality test
  }

  /*
   * Reproducer for bug documented in DH-18265
   */
  public void testDH18265() {
    KeyedLongTestObject result;

    final KeyedLongTestObjectMap m = new KeyedLongTestObjectMap(10);

    // Setup the conditions for the bug to be triggered.
    final int capacity = m.capacity();

    // This will hash to 0 internally
    final KeyedLongTestObject o1 = new KeyedLongTestObject(capacity);
    result = m.putIfAbsent(capacity, o1);
    assertNull(result);
    assertSame(m.get(capacity), o1); // strict equality test

    // This will also initially hash to 0, but will be double hashed to an empty slot.
    final KeyedLongTestObject o2 = new KeyedLongTestObject(0);
    result = m.putIfAbsent(0, o2);
    assertNull(result);
    assertSame(m.get(0), o2); // strict equality test

    // Remove the first object, leaving a DELETED tombstone at the 0 slot.
    result = m.remove(capacity);
    assertNotNull(result);
    assertSame(result, o1); // strict equality test

    // This replace should fail, since we do not match old values.
    final KeyedLongTestObject o3 = new KeyedLongTestObject(10);
    final KeyedLongTestObject o4 = new KeyedLongTestObject(0);
    assertFalse(m.replace(0, o3, o4));
    assertSame(m.get(0), o2); // strict equality test

    // This replace should succeed, since we match the old value.
    assertTrue(m.replace(0, o2, o4));
    assertSame(m.get(0), o4); // strict equality test
  }

  /*
   ** tests for KeyedLongObjectMaps -- putIfAbsent(K, ValueFactory)
   */
  public class KIOMPutIfAbsent<V> extends Thread {
    public final int numRuns;
    public final HashMap<Long, V> objects;
    public final KeyedLongObjectHash<V> map;
    public final KeyedLongObjectHash.ValueFactory<V> factory;

    public KIOMPutIfAbsent(
        int numRuns,
        HashMap<Long, V> objects,
        KeyedLongObjectHash<V> map,
        KeyedLongObjectHash.ValueFactory<V> factory) {
      this.numRuns = numRuns;
      this.objects = objects;
      this.map = map;
      this.factory = factory;
    }

    public int numRemoves = 0;

    public void run() {
      for (int i = 0; i < numRuns; ++i) {
        for (Long k : objects.keySet()) {
          map.putIfAbsent(k.longValue(), factory); // make sure we call the right method!
        }
      }
      for (Long k : objects.keySet()) {
        if (random.nextDouble() < 0.4) {
          if (map.removeKey(k.longValue()) != null) { // make sure we call the right method!
            ++numRemoves;
          }
        }
        if (random.nextDouble() < 0.01) {
          compact((Map<Long, V>) map);
        }
      }
      for (Long k : objects.keySet()) {
        map.putIfAbsent(k.longValue(), factory); // make sure we call the right method!
      }
    }
  }

  private static class KIOMPutIfAbsentFactory<V> implements KeyedLongObjectHash.ValueFactory<V> {
    public final HashMap<Long, V> objects;

    public KIOMPutIfAbsentFactory(HashMap<Long, V> objects) {
      this.objects = objects;
    }

    public int numCalls = 0;

    public V newValue(Long key) {
      ++numCalls;
      return objects.get(key);
    }

    public V newValue(long key) {
      ++numCalls;
      return objects.get(key);
    }
  }

  public void testKIOMPutIfAbsent() {
    final Map<Long, KeyedLongTestObject> map = newTestMap(10, null);
    if (!(map instanceof KeyedLongObjectHash)) {
      return;
    }
    KeyedLongObjectHash<KeyedLongTestObject> KIOM =
        ((KeyedLongObjectHash<KeyedLongTestObject>) map);

    final HashMap<Long, KeyedLongTestObject> objects =
        generateUniqueRandomHashMap(SIZE * 10, MIN_KEY, MAX_KEY);
    final KIOMPutIfAbsentFactory<KeyedLongTestObject> factory =
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
    final Map<Long, KeyedLongTestObject> map = newTestMap(10, null);
    if (!(map instanceof KeyedLongObjectHash)) {
      return;
    }
    final KeyedLongObjectHash<KeyedLongTestObject> SUT =
        ((KeyedLongObjectHash<KeyedLongTestObject>) map);

    final long MILLIS = 1000;

    Thread setter =
        new Thread() {
          @Override
          public void run() {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() < t0 + MILLIS) {
              for (int i = 0; i < SUT.capacity(); ++i) {
                SUT.put(i, new KeyedLongTestObject(i));
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

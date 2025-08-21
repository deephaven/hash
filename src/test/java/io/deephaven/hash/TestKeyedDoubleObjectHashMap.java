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

public class TestKeyedDoubleObjectHashMap
    extends AbstractTestGenericMap<Double, KeyedDoubleTestObject> {
  private static Logger log = LoggerFactory.getLogger(TestKeyedDoubleObjectHashMap.class);

  public TestKeyedDoubleObjectHashMap(String name) {
    super(name, 100);
  }

  private static Random random = new Random(101763);

  public HashMap<Double, KeyedDoubleTestObject> generateUniqueRandomHashMap(
      int size, int min_key, int max_key) {
    HashMap<Double, KeyedDoubleTestObject> m = new HashMap<>(size);
    assert min_key < max_key;
    assert max_key - min_key > size;
    while (m.size() != size) {
      double key = (double) random.nextInt(max_key - min_key) + min_key;
      if (!m.containsKey(key)) {
        m.put(key, new KeyedDoubleTestObject(key));
      }
    }
    return m;
  }

  protected Map<Double, KeyedDoubleTestObject> newTestMap(
      int initialSize, Map<Double, KeyedDoubleTestObject> from) {
    KeyedDoubleTestObjectMap map = new KeyedDoubleTestObjectMap(initialSize);
    if (from != null) {
      for (KeyedDoubleTestObject o : from.values()) {
        map.put(o.getId(), o);
      }
    }
    return map;
  }

  protected void compact(Map map) {
    assert map instanceof KeyedDoubleTestObjectMap;
    ((KeyedDoubleTestObjectMap) map).compact();
  }

  protected KeyedDoubleTestObject[] newValueArray(int n) {
    return new KeyedDoubleTestObject[n];
  }

  protected Double[] newKeyArray(int n) {
    return new Double[n];
  }

  protected KeyedDoubleTestObject newValue(Double key) {
    return new KeyedDoubleTestObject(key);
  }

  protected Double getKey(KeyedDoubleTestObject value) {
    return value.getId();
  }

  /**
   * If the test subject is an indexable map, make sure the getByIndex method returns identical
   * objects
   */
  protected <K, V> void assertConsistency(Map<K, V> subject) {
    assert subject instanceof KeyedDoubleTestObjectMap;
    IndexableMap<Integer, KeyedDoubleTestObject> imap =
        (IndexableMap<Integer, KeyedDoubleTestObject>) subject;
    for (int i = 0; i < imap.size(); ++i) {
      KeyedDoubleTestObject o = imap.getByIndex(i);
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
    final KeyedDoubleTestObjectMap m = new KeyedDoubleTestObjectMap(capacity, loadFactor);

    for (int i = 0; i < capacity * 2; ++i) {
      if (m.size() >= m.capacity() - 1) {
        // remove the first key
        final KeyedDoubleTestObject o = m.getByIndex(0);
        m.remove(o.getId());
      }

      // add a random key
      final long key = random.nextInt(10);
      final KeyedDoubleTestObject o = new KeyedDoubleTestObject((double) key);
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
    final KeyedDoubleTestObjectMap m1 = new KeyedDoubleTestObjectMap();
    KeyedDoubleTestObject o1 = new KeyedDoubleTestObject(1);
    KeyedDoubleTestObject o2 = new KeyedDoubleTestObject(1);

    boolean replaced;

    replaced = m1.replace(1, o2, o1);
    assertFalse("replace() should not return true when value not found", replaced);
    assertSame(m1.get(1), null); // confirm replace failed

    m1.add(o1);
    replaced = m1.replace(1, o1, o2);
    assertTrue("replace() should return true when value found and replaced", replaced);
    assertSame(m1.get(1), o2);

    // Repeat using boxed datatype
    final KeyedDoubleTestObjectMap m2 = new KeyedDoubleTestObjectMap();
    final Double d1 = 1.0;

    replaced = m2.replace(d1, o1, o2);
    assertFalse("replace() should not return true when value not found", replaced);
    assertSame(m2.get(d1), null); // confirm replace failed

    m2.add(o1);
    replaced = m2.replace(d1, o2, o1);
    assertTrue("replace() should return true when value found and replaced", replaced);
    assertSame(m2.get(d1), o1);
  }

  /*
   ** tests the unboxed putIfAbsent call
   */
  public void testSimpleUnboxedPutIfAbsent() {
    final KeyedDoubleTestObjectMap m = new KeyedDoubleTestObjectMap();

    // create two objects that are equals() but not identical
    KeyedDoubleTestObject o1 = new KeyedDoubleTestObject(42);
    KeyedDoubleTestObject o2 = new KeyedDoubleTestObject(42);

    KeyedDoubleTestObject result;

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
    KeyedDoubleTestObject result;

    final KeyedDoubleTestObjectMap m = new KeyedDoubleTestObjectMap(10);

    final KeyedDoubleTestObject o1 = new KeyedDoubleTestObject(0);
    final KeyedDoubleTestObject o2 = new KeyedDoubleTestObject(0);
    final KeyedDoubleTestObject o3 = new KeyedDoubleTestObject(0);

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

    assertFalse(m.replace(0, new KeyedDoubleTestObject(10), o3));
    assertSame(m.get(0), o2); // strict equality test

    assertTrue(m.replace(0, new KeyedDoubleTestObject(0), o3));
    assertSame(m.get(0), o3); // strict equality test
  }

  /*
   * Reproducer for bug documented in DH-18265
   */
  public void testDH18265() {
    KeyedDoubleTestObject result;

    final KeyedDoubleTestObjectMap m = new KeyedDoubleTestObjectMap(2);

    // Setup the conditions for the bug to be triggered. Not all powers of two work, but this does.
    final double initial = 2 << 16;
    final double collision = initial + m.capacity();

    final KeyedDoubleTestObject o1 = new KeyedDoubleTestObject(initial);
    result = m.putIfAbsent(initial, o1);
    assertNull(result);
    assertSame(m.get(initial), o1); // strict equality test

    // This will also initially collide, but will be double hashed to an empty slot.
    final KeyedDoubleTestObject o2 = new KeyedDoubleTestObject(collision);
    result = m.putIfAbsent(collision, o2);
    assertNull(result);
    assertSame(m.get(collision), o2); // strict equality test

    // Remove the first object, leaving a DELETED tombstone at the slot.
    result = m.remove(initial);
    assertNotNull(result);
    assertSame(result, o1); // strict equality test

    // This replace should fail, since we do not match old values.
    final KeyedDoubleTestObject o3 = new KeyedDoubleTestObject(10);
    final KeyedDoubleTestObject o4 = new KeyedDoubleTestObject(collision);
    assertFalse(m.replace(collision, o3, o4));
    assertSame(m.get(collision), o2); // strict equality test

    // This replace should succeed, since we match the old value.
    assertTrue(m.replace(collision, o2, o4));
    assertSame(m.get(collision), o4); // strict equality test
  }

  /*
   ** tests for KeyedIntObjectMaps -- putIfAbsent(K, ValueFactory)
   */
  public class KIOMPutIfAbsent<V> extends Thread {
    public final int numRuns;
    public final HashMap<Double, V> objects;
    public final KeyedDoubleObjectHash<V> map;
    public final KeyedDoubleObjectHash.ValueFactory<V> factory;

    public KIOMPutIfAbsent(
        int numRuns,
        HashMap<Double, V> objects,
        KeyedDoubleObjectHash<V> map,
        KeyedDoubleObjectHash.ValueFactory<V> factory) {
      this.numRuns = numRuns;
      this.objects = objects;
      this.map = map;
      this.factory = factory;
    }

    public int numRemoves = 0;

    public void run() {
      for (int i = 0; i < numRuns; ++i) {
        for (Double k : objects.keySet()) {
          map.putIfAbsent(k.intValue(), factory); // make sure we call the right method!
        }
      }
      for (Double k : objects.keySet()) {
        if (random.nextDouble() < 0.4) {
          if (map.removeKey(k.intValue()) != null) { // make sure we call the right method!
            ++numRemoves;
          }
        }
        if (random.nextDouble() < 0.01) {
          compact((Map<Integer, V>) map);
        }
      }
      for (Double k : objects.keySet()) {
        map.putIfAbsent(k.intValue(), factory); // make sure we call the right method!
      }
    }
  }

  private static class KIOMPutIfAbsentFactory<V> implements KeyedDoubleObjectHash.ValueFactory<V> {
    public final HashMap<Double, V> objects;

    public KIOMPutIfAbsentFactory(HashMap<Double, V> objects) {
      this.objects = objects;
    }

    public int numCalls = 0;

    public V newValue(Double key) {
      ++numCalls;
      return objects.get(key);
    }

    public V newValue(double key) {
      ++numCalls;
      return objects.get(key);
    }
  }

  public void testKIOMPutIfAbsent() {
    final Map<Double, KeyedDoubleTestObject> map = newTestMap(10, null);
    if (!(map instanceof KeyedDoubleObjectHash)) {
      return;
    }
    KeyedDoubleObjectHash<KeyedDoubleTestObject> KIOM =
        ((KeyedDoubleObjectHash<KeyedDoubleTestObject>) map);

    final HashMap<Double, KeyedDoubleTestObject> objects =
        generateUniqueRandomHashMap(SIZE * 10, MIN_KEY, MAX_KEY);
    final KIOMPutIfAbsentFactory<KeyedDoubleTestObject> factory =
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
    final Map<Double, KeyedDoubleTestObject> map = newTestMap(10, null);
    if (!(map instanceof KeyedIntObjectHash)) {
      return;
    }
    final KeyedIntObjectHash<KeyedDoubleTestObject> SUT =
        ((KeyedIntObjectHash<KeyedDoubleTestObject>) map);

    final long MILLIS = 1000;

    Thread setter =
        new Thread() {
          @Override
          public void run() {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() < t0 + MILLIS) {
              for (int i = 0; i < SUT.capacity(); ++i) {
                SUT.put(i, new KeyedDoubleTestObject(i));
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

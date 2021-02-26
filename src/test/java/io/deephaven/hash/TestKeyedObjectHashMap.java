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
public class TestKeyedObjectHashMap extends AbstractTestGenericMap<String, KeyedTestObject> {
  private static Logger log = LoggerFactory.getLogger(TestKeyedObjectHashMap.class);

  public TestKeyedObjectHashMap(String name) {
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
    KeyedTestObjectMap map = new KeyedTestObjectMap(initialSize);
    if (from != null) {
      for (KeyedTestObject o : from.values()) {
        map.put(o.getId(), o);
      }
    }
    return map;
  }

  protected void compact(Map map) {
    assert map instanceof KeyedTestObjectMap;
    ((KeyedTestObjectMap) map).compact();
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
    assert subject instanceof KeyedTestObjectMap;
    IndexableMap<String, KeyedTestObject> imap = (IndexableMap<String, KeyedTestObject>) subject;
    for (int i = 0; i < imap.size(); ++i) {
      KeyedTestObject o = imap.getByIndex(i);
      assertTrue("values are identical", o == subject.get(o.getId()));
    }
  }

  /*
   ** tests for KeyedObjectMaps -- putIfAbsent(K, ValueFactory)
   */
  public class KOMPutIfAbsent extends Thread {
    public final int numRuns;
    public final HashMap<String, KeyedTestObject> objects;
    public final KeyedObjectHashMap<String, KeyedTestObject> map;
    public final KeyedObjectHash.ValueFactory<String, KeyedTestObject> factory;

    public KOMPutIfAbsent(
        int numRuns,
        HashMap<String, KeyedTestObject> objects,
        KeyedObjectHashMap<String, KeyedTestObject> map,
        KeyedObjectHash.ValueFactory<String, KeyedTestObject> factory) {
      this.numRuns = numRuns;
      this.objects = objects;
      this.map = map;
      this.factory = factory;
    }

    public int numRemoves = 0;

    public void run() {
      for (int i = 0; i < numRuns; ++i) {
        for (String k : objects.keySet()) {
          map.putIfAbsent(k, factory);
        }
      }
      if (true) {
        for (String k : objects.keySet()) {
          if (random.nextDouble() < 0.4) {
            if (map.removeKey(k) != null) {
              ++numRemoves;
            }
          }
          if (random.nextDouble() < 0.01) {
            compact(map);
          }
        }
        for (String k : objects.keySet()) {
          map.putIfAbsent(k, factory);
        }
      }
    }
  }

  private static class KOMPutIfAbsentFactory<K, V> implements KeyedObjectHash.ValueFactory<K, V> {
    public final HashMap<K, V> objects;

    public KOMPutIfAbsentFactory(HashMap<K, V> objects) {
      this.objects = objects;
    }

    public int numCalls = 0;

    public V newValue(K key) {
      ++numCalls;
      return objects.get(key);
    }
  }

  public void testKOMPutIfAbsent() {
    final Map<String, KeyedTestObject> map = newTestMap(10, null);
    if (!(map instanceof KeyedObjectHashMap)) {
      return;
    }
    KeyedObjectHashMap<String, KeyedTestObject> kohm =
        ((KeyedObjectHashMap<String, KeyedTestObject>) map);

    final HashMap<String, KeyedTestObject> objects =
        generateUniqueRandomHashMap(SIZE * 10, MIN_KEY, MAX_KEY);
    final KOMPutIfAbsentFactory<String, KeyedTestObject> factory =
        new KOMPutIfAbsentFactory<String, KeyedTestObject>(objects);
    final int NUM_THREADS = 5;
    final int NUM_RUNS = 100;

    ArrayList<KOMPutIfAbsent> mutators = new ArrayList<KOMPutIfAbsent>(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
      mutators.add(new KOMPutIfAbsent(NUM_RUNS, objects, kohm, factory));
    }
    for (int i = 0; i < NUM_THREADS; ++i) {
      mutators.get(i).start();
    }
    int totalRemoves = 0;
    for (int i = 0; i < NUM_THREADS; ++i) {
      while (true) {
        try {
          KOMPutIfAbsent m = mutators.get(i);
          m.join();
          totalRemoves += m.numRemoves;
          log.info("testKOMPutIfAbsent: mutator " + i + " had " + m.numRemoves + " removes");
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
    assertEquals(objects.size() + totalRemoves, factory.numCalls);
  }

  private static class SelfBoxedInt {
    final int value;

    public SelfBoxedInt(int value) {
      this.value = value;
    }

    static KeyedObjectKey<SelfBoxedInt, SelfBoxedInt> KEY =
        new KeyedObjectKey<SelfBoxedInt, SelfBoxedInt>() {
          @Override
          public SelfBoxedInt getKey(SelfBoxedInt selfBoxedInt) {
            return selfBoxedInt;
          }

          @Override
          public int hashKey(SelfBoxedInt selfBoxedInt) {
            return selfBoxedInt.value;
          }

          @Override
          public boolean equalKey(SelfBoxedInt selfBoxedInt, SelfBoxedInt selfBoxedInt1) {
            return selfBoxedInt.value == selfBoxedInt1.value;
          }
        };
  }

  public void testKOMConcurrentGet() {
    final KeyedObjectHash<SelfBoxedInt, SelfBoxedInt> SUT =
        new KeyedObjectHash<SelfBoxedInt, SelfBoxedInt>(SelfBoxedInt.KEY);

    final long MILLIS = 1000;

    final SelfBoxedInt[] obs = new SelfBoxedInt[SUT.capacity()];
    for (int i = 0; i < obs.length; ++i) {
      obs[i] = new SelfBoxedInt(i);
    }

    Thread setter =
        new Thread() {
          @Override
          public void run() {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() < t0 + MILLIS) {
              for (int i = 0; i < SUT.capacity(); ++i) {
                SUT.put(obs[i], obs[i]);
                SUT.removeKey(obs[i]);
              }
            }
          }
        };

    setter.start();

    long t0 = System.currentTimeMillis();
    try {
      while (System.currentTimeMillis() < t0 + MILLIS) {
        // doesn't matter what key we get here - just has to be
        SUT.get(obs[0]);
      }
      setter.join();
    } catch (Exception x) {
      fail("Unhandled exception: " + x);
    }
  }
}

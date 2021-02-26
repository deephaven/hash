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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestGenericMap<K, V> extends TestCase {
  protected static Logger log = LoggerFactory.getLogger(AbstractTestGenericMap.class);

  protected final int SIZE;
  protected final int MIN_KEY;
  protected final int MAX_KEY;

  public AbstractTestGenericMap(String name) {
    this(name, 100);
  }

  public AbstractTestGenericMap(String name, int size) {
    super(name);
    if (size < 10) {
      throw new IllegalArgumentException("size must be greater than 10");
    }
    SIZE = size;
    MIN_KEY = size * -5000;
    MAX_KEY = size * 5000;
  }

  protected static Random random = new Random(101763);

  /**
   * Generates a HashMap with random keys to use as a reference. The min_key and max_key parameters
   * can be treated literally if the map keys are integers, otherwise they should be used as an
   * indiciation of the portion of the key domain which should be covered by the new map
   *
   * @param size the desired size of the map
   * @param min_key the minimum key value
   * @param max_key the maximum key value
   * @return a hash map initialized with random keys and values
   */
  protected abstract HashMap<K, V> generateUniqueRandomHashMap(int size, int min_key, int max_key);

  /**
   * Create a new instance of the map implementation under test. If the from argument is not null,
   * then its entries should be copied into the new map.
   *
   * @param initialSize the initialSize argument to the map constructor
   * @param from the map containing the entries to copy
   * @return a new instance of the map implementation under test
   */
  protected abstract Map<K, V> newTestMap(int initialSize, Map<K, V> from);

  /** If the implementation under test supports a compact or trimToSize() method, call it. */
  protected abstract void compact(Map<K, V> map);

  /** Return a correctly-typed array of V[] */
  protected abstract V[] newValueArray(int n);

  /** Return a correctly-typed array of K[] */
  protected abstract K[] newKeyArray(int n);

  /** Create a new instance of the value class V, based on the given key */
  protected abstract V newValue(K key);

  /** Given a value, return a corresponding key. */
  protected abstract K getKey(V value);

  /**
   * Split a map, with frac percent of the entries going into map a and 1-frac percent of the keys
   * to map b. Either a or b may be null, in which case the corresponding entries are discarded.
   *
   * @param map the map to be split
   * @param frac the fraction of keys going into the first target map
   * @param a the first target map, receiving approximately frac percent of the original entries
   * @param b the first target map, receiving approximately 1-frac percent of the original entries
   */
  protected static <K, V> void randomMapSplit(
      Map<K, V> map, double frac, Map<K, V> a, Map<K, V> b) {
    for (Map.Entry<K, V> e : map.entrySet()) {
      if (random.nextDouble() < frac) {
        if (a != null) {
          a.put(e.getKey(), e.getValue());
        }
      } else if (b != null) {
        b.put(e.getKey(), e.getValue());
      }
    }
  }

  /**
   * This is called after each modification; the subclass can implement additional consistency
   * checks here.
   */
  protected <K, V> void assertConsistency(Map<K, V> subject) {
    // empty
  }

  /**
   * Compare two maps for equality, using as many methods as we can.
   *
   * @param subject the test subject
   * @param reference the reference map
   */
  public <K, V> void assertMapsEqual(Map<K, V> subject, Map<K, V> reference) {
    assertEquals(subject.isEmpty(), reference.isEmpty());
    assertTrue(subject.size() == 0 || !subject.isEmpty());
    assertTrue(subject.size() != 0 || subject.isEmpty());

    assertEquals(subject.size(), reference.size());
    assertEquals(subject.keySet().size(), reference.size());
    assertEquals(subject.values().size(), reference.size());
    assertEquals(subject.entrySet().size(), reference.size());

    assertEquals(subject, reference);
    assertEquals(subject.keySet(), reference.keySet());
    assertEquals(subject.entrySet(), reference.entrySet());

    assertEquals(subject.hashCode(), reference.hashCode());
    assertEquals(subject.keySet().hashCode(), reference.keySet().hashCode());
    assertEquals(subject.entrySet().hashCode(), reference.entrySet().hashCode());

    // assertCollectionContainsExactly(subject.values(), (V[]) reference.values().toArray());
    // assertCollectionContainsExactly(subject.keySet(), (K[]) reference.keySet().toArray());
    // assertCollectionContainsExactly(subject.entrySet(), (Map.Entry<K, V>[])
    // reference.entrySet().toArray(new Map.Entry[0]));

    for (K k : reference.keySet()) {
      V v = reference.get(k);
      assertEquals(v, subject.get(k));
    }
  }

  // ----------------------------------------------------------------
  /** Asserts that the given collection contains exactly the given elements (in any order). */
  public <E> void assertCollectionContainsExactly(
      Collection<E> collectionToSearch, E... itemsToFind) {
    assertCollectionContainsExactly(null, collectionToSearch, itemsToFind);
  }

  // ----------------------------------------------------------------
  /** Asserts that the given collection contains exactly the given elements (in any order). */
  public <E> void assertCollectionContainsExactly(
      String sMessage, Collection<E> collectionToSearch, E... itemsToFind) {
    try {
      String sPrefix = null == sMessage ? "" : sMessage + " ";
      if (null == itemsToFind) {
        assertNull(sPrefix + "Expected collectionToSearch to be null.", collectionToSearch);
      } else {
        assertNotNull(sPrefix + "Expected collectionToSearch to be non-null.", collectionToSearch);
        assertEquals(
            sPrefix + "Expected collectionToSearch and itemsToFind to be the same size.",
            itemsToFind.length,
            collectionToSearch.size());
        for (E item : itemsToFind) {
          assertTrue(
              sPrefix + "Expected collectionToSeach to contain \"" + item + "\".",
              collectionToSearch.contains(item));
        }
      }
    } catch (AssertionFailedError e) {
      System.err.println("Expected (" + itemsToFind.length + " items):");
      for (E item : itemsToFind) {
        System.err.println("    " + item);
      }
      System.err.println("Actual (" + collectionToSearch.size() + " items):");
      for (E item : collectionToSearch) {
        System.err.println("    " + item);
      }
      throw e;
    }
  }

  /**
   * Test whether the values in two maps are identical, not just equal
   *
   * @param subject the test subject
   * @param reference the reference map
   * @return true, if the map values are identical
   */
  public <K, V> boolean mapValuesIdentical(Map<K, V> subject, Map<K, V> reference) {
    if (subject.size() != reference.size()) {
      return false;
    }
    for (K k : subject.keySet()) {
      V v = subject.get(k);
      V other_v = reference.get(k);
      if (v != other_v) {
        return false;
      }
    }
    return true;
  }

  /**
   * Put lots of data into an instance of the implementation under test.
   *
   * @throws Exception
   */
  public void testPut() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), null);
    for (Map.Entry<K, V> e : objects.entrySet()) {
      map.put(e.getKey(), e.getValue());
    }
    assertMapsEqual(map, objects);
    assertTrue(mapValuesIdentical(map, objects));
    assertConsistency(map);
  }

  public void testPutAll() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), null);
    map.putAll(objects);
    assertMapsEqual(map, objects);
    assertTrue(mapValuesIdentical(map, objects));
    assertConsistency(map);
  }

  public void testUpdate() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);
    assertMapsEqual(map, objects);
    assertTrue(mapValuesIdentical(map, objects));

    HashMap<K, V> objects2 = new HashMap<K, V>(objects.size());
    for (K k : objects.keySet()) {
      V new_v = newValue(k);
      objects2.put(k, new_v);
      map.put(k, new_v);
    }
    assertMapsEqual(objects, objects2);

    assertMapsEqual(map, objects);
    assertMapsEqual(map, objects2);
    assertFalse(mapValuesIdentical(map, objects));
    assertTrue(mapValuesIdentical(map, objects2));
    assertConsistency(map);
  }

  public void testUpdateAll() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);
    assertTrue(mapValuesIdentical(map, objects));

    HashMap<K, V> objects2 = new HashMap<K, V>(objects.size());
    for (K k : objects.keySet()) {
      V new_v = newValue(k);
      objects2.put(k, new_v);
    }
    assertMapsEqual(objects, objects2);

    map.putAll(objects2);
    assertMapsEqual(map, objects);
    assertMapsEqual(map, objects2);
    assertFalse(mapValuesIdentical(map, objects));
    assertTrue(mapValuesIdentical(map, objects2));
    assertConsistency(map);
  }

  public void testGet() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    HashMap<K, V> moreObjects = generateUniqueRandomHashMap(SIZE * 10, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), null);
    for (V o : objects.values()) {
      map.put(getKey(o), o);
    }
    for (V o : objects.values()) {
      assertTrue(objects.get(getKey(o)) == o);
      assertTrue(map.get(getKey(o)) == o);
    }
    for (V o : moreObjects.values()) {
      assertEquals(objects.get(getKey(o)), map.get(getKey(o)));
    }
  }

  public void testLongLived() {
    Map<K, V> map = newTestMap(10, null);
    HashMap<K, V> hmap = new HashMap<K, V>(10);

    for (int i = 0; i < 10; ++i) {
      HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE * 10, 1, MAX_KEY / 25);
      map.putAll(objects);
      hmap.putAll(objects);
      assertMapsEqual(map, hmap);
      assertTrue(mapValuesIdentical(map, hmap));

      HashMap<K, V> objects2 = generateUniqueRandomHashMap(SIZE, 1, MAX_KEY / 25);
      map.keySet().removeAll(objects2.keySet());
      hmap.keySet().removeAll(objects2.keySet());
      assertMapsEqual(map, hmap);
      assertTrue(mapValuesIdentical(map, hmap));
    }

    assertCollectionContainsExactly(map.values(), hmap.values().toArray(newValueArray(0)));
    assertCollectionContainsExactly(map.keySet(), hmap.keySet().toArray(newKeyArray(0)));
    assertCollectionContainsExactly(
        map.entrySet(), (Map.Entry<K, V>[]) hmap.entrySet().toArray(new Map.Entry[0]));
    assertConsistency(map);
  }

  public void testRehash() {
    HashMap<K, V> objects1 = generateUniqueRandomHashMap(SIZE, -(SIZE / 2 + 1), SIZE / 2 + 1);
    HashMap<K, V> objects2 = generateUniqueRandomHashMap(SIZE, 0, SIZE + 1);
    HashMap<K, V> objects3 = generateUniqueRandomHashMap(SIZE * 2, -(SIZE + 1), SIZE + 1);
    HashMap<K, V> objects4 = generateUniqueRandomHashMap(SIZE * 10, -(SIZE * 10), SIZE * 10);
    HashMap<K, V> objects5 = generateUniqueRandomHashMap(SIZE * 50, -(SIZE * 30), SIZE * 30);

    Map<K, V> map = newTestMap(10, null);
    HashMap<K, V> hmap = new HashMap<K, V>();

    map.putAll(objects1);
    hmap.putAll(objects1);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects2);
    hmap.putAll(objects2);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects3);
    hmap.putAll(objects3);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects4);
    hmap.putAll(objects4);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects5);
    hmap.putAll(objects5);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    Set<K> keys = map.keySet();

    keys.removeAll(objects5.keySet());
    hmap.keySet().removeAll(objects5.keySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    keys.removeAll(objects4.keySet());
    hmap.keySet().removeAll(objects4.keySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    keys.removeAll(objects3.keySet());
    hmap.keySet().removeAll(objects3.keySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    keys.removeAll(objects2.keySet());
    hmap.keySet().removeAll(objects2.keySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    keys.removeAll(objects1.keySet());
    hmap.keySet().removeAll(objects1.keySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    assertEquals(map.size(), 0);
    assertTrue(map.isEmpty());
    assertConsistency(map);

    /*
     ** now build it back up again, and remove from the entry set
     */

    map.putAll(objects1);
    hmap.putAll(objects1);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects2);
    hmap.putAll(objects2);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects3);
    hmap.putAll(objects3);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects4);
    hmap.putAll(objects4);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    map.putAll(objects5);
    hmap.putAll(objects5);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    Set<Map.Entry<K, V>> ents = map.entrySet();

    ents.removeAll(objects5.entrySet());
    hmap.entrySet().removeAll(objects5.entrySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    ents.removeAll(objects4.entrySet());
    hmap.entrySet().removeAll(objects4.entrySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    ents.removeAll(objects3.entrySet());
    hmap.entrySet().removeAll(objects3.entrySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    ents.removeAll(objects2.entrySet());
    hmap.entrySet().removeAll(objects2.entrySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    ents.removeAll(objects1.entrySet());
    hmap.entrySet().removeAll(objects1.entrySet());
    compact(map);
    assertMapsEqual(map, hmap);
    assertTrue(mapValuesIdentical(map, hmap));
    assertConsistency(map);

    assertEquals(map.size(), 0);
    assertTrue(map.isEmpty());
  }

  public void testRemove() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);

    HashMap<K, V> retained = new HashMap<K, V>();
    HashMap<K, V> removed = new HashMap<K, V>();
    randomMapSplit(objects, 0.5, retained, removed);

    for (K k : removed.keySet()) {
      map.remove(k);
      assertFalse(map.containsKey(k));
    }
    assertMapsEqual(map, retained);
    assertTrue(mapValuesIdentical(map, retained));
    assertConsistency(map);
  }

  public void testKeySetRemove() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);

    HashMap<K, V> retained = new HashMap<K, V>();
    HashMap<K, V> removed = new HashMap<K, V>();
    randomMapSplit(objects, 0.5, retained, removed);

    Set<K> keys = map.keySet();
    for (K k : removed.keySet()) {
      keys.remove(k);
      assertFalse(map.containsKey(k));
      assertFalse(keys.contains(k));
    }
    assertMapsEqual(map, retained);
    assertTrue(mapValuesIdentical(map, retained));
    assertConsistency(map);
  }

  public void testKeySetRemoveAll() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);

    HashMap<K, V> retained = new HashMap<K, V>();
    HashMap<K, V> removed = new HashMap<K, V>();
    randomMapSplit(objects, 0.5, retained, removed);

    map.keySet().removeAll(removed.keySet());

    assertMapsEqual(map, retained);
    assertTrue(mapValuesIdentical(map, retained));
    assertConsistency(map);
  }

  public void testEntrySetRemove() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);

    HashMap<K, V> retained = new HashMap<K, V>();
    HashMap<K, V> removed = new HashMap<K, V>();
    randomMapSplit(objects, 0.5, retained, removed);

    Set<Map.Entry<K, V>> entries = map.entrySet();
    for (Map.Entry<K, V> e : removed.entrySet()) {
      entries.remove(e);
      assertFalse(map.containsKey(e.getKey()));
      assertFalse(entries.contains(e));
    }
    assertMapsEqual(map, retained);
    assertTrue(mapValuesIdentical(map, retained));
    assertConsistency(map);
  }

  public void testEntrySetRemoveAll() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);

    HashMap<K, V> retained = new HashMap<K, V>();
    HashMap<K, V> removed = new HashMap<K, V>();
    randomMapSplit(objects, 0.5, retained, removed);

    map.entrySet().removeAll(removed.entrySet());

    assertMapsEqual(map, retained);
    assertTrue(mapValuesIdentical(map, retained));
    assertConsistency(map);
  }

  public void testKeyIteratorRemove() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);

    HashMap<K, V> retained = (HashMap<K, V>) objects.clone();
    int nr = 0, ni = 0;
    for (Iterator<K> i = map.keySet().iterator(); i.hasNext(); ) {
      K k = i.next();
      assertTrue(retained.containsKey(k));
      assertTrue(map.containsKey(k));
      if (random.nextDouble() < 0.5) {
        retained.remove(k);
        i.remove();
        ++nr;
        assertFalse(retained.containsKey(k));
        assertFalse(map.containsKey(k));
      }
      ++ni;
    }
    assertEquals(ni, objects.size());
    assertEquals(retained.size(), objects.size() - nr);
    assertEquals(map.size(), objects.size() - nr);
    assertMapsEqual(map, retained);
    assertTrue(mapValuesIdentical(map, retained));
    assertConsistency(map);
  }

  public void testEntryIteratorRemove() throws Exception {
    HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    Map<K, V> map = newTestMap(objects.size(), objects);

    assertMapsEqual(map, objects);

    HashMap<K, V> retained = (HashMap<K, V>) objects.clone();
    int nr = 0, ni = 0;
    for (Iterator<Map.Entry<K, V>> i = map.entrySet().iterator(); i.hasNext(); ) {
      Map.Entry<? extends K, ? extends V> e = i.next();
      assertTrue(retained.containsKey(e.getKey()));
      assertTrue(map.containsKey(e.getKey()));
      assertTrue(retained.containsValue(e.getValue()));
      assertTrue(map.containsValue(e.getValue()));

      if (random.nextDouble() < 0.5) {
        retained.remove(e.getKey());
        i.remove();
        ++nr;
        assertFalse(retained.containsKey(e.getKey()));
        assertFalse(map.containsKey(e.getKey()));
        assertFalse(retained.containsValue(e.getValue()));
        assertFalse(map.containsValue(e.getValue()));
      }
      ++ni;
    }
    assertEquals(ni, objects.size());
    assertEquals(retained.size(), objects.size() - nr);
    assertEquals(map.size(), objects.size() - nr);
    assertMapsEqual(map, retained);
    assertTrue(mapValuesIdentical(map, retained));
    assertConsistency(map);
  }

  /*
   ** tests for ConcurrentMaps
   */
  public void testSimplePutIfAbsent() {
    final Map<K, V> map = newTestMap(10, null);
    if (!(map instanceof ConcurrentMap)) {
      return;
    }

    final ConcurrentMap<K, V> cmap = (ConcurrentMap<K, V>) map;
    final ConcurrentHashMap<K, V> chash = new ConcurrentHashMap<K, V>();

    // create two objects that are equals() but not identical
    final HashMap<K, V> objects = generateUniqueRandomHashMap(1, MIN_KEY, MAX_KEY);
    V o1 = objects.values().iterator().next();
    V o2 = newValue(getKey(o1));
    assertTrue(o1 != o2);
    assertTrue(o1.equals(o2));

    V mResult, hResult;

    mResult = cmap.putIfAbsent(getKey(o1), o1);
    hResult = chash.putIfAbsent(getKey(o1), o1);
    assertTrue(mResult == null);
    assertTrue(hResult == null);

    mResult = cmap.putIfAbsent(getKey(o2), o2);
    hResult = cmap.putIfAbsent(getKey(o2), o2);
    assertTrue(mResult == o1);
    assertTrue(hResult == o1);

    assertTrue(cmap.get(getKey(o2)) == o1);
    assertTrue(chash.get(getKey(o2)) == o1);
  }

  public class ConcurrentPutMutator extends Thread {
    public boolean done = false;
    public final HashMap<K, V> objects;
    public final Map<K, V> map;

    ConcurrentPutMutator(HashMap<K, V> objects, Map<K, V> map) {
      this.objects = objects;
      this.map = map;
    }

    public void run() {
      for (int i = 0; i < 10; ++i) {
        for (K k : objects.keySet()) {
          map.put(k, newValue(k));
        }
        for (K k : objects.keySet()) {
          if (random.nextDouble() < 0.2) {
            map.remove(k);
          }
          if (random.nextDouble() < 0.01) {
            compact(map);
          }
        }
        for (K k : objects.keySet()) {
          map.put(k, newValue(k));
        }
      }
      synchronized (this) {
        done = true;
        notifyAll();
      }
    }

    public synchronized boolean isDone() {
      return done;
    }
  }

  public class ConcurrentPutAccessor extends Thread {
    public final HashMap<K, V> objects;
    public final Map<K, V> map;
    public volatile boolean stop = false;
    public boolean done = false;
    int accessCount = 0;
    int errorCount = 0;
    int nullCount = 0;

    ConcurrentPutAccessor(HashMap<K, V> objects, Map<K, V> map) {
      this.objects = objects;
      this.map = map;
    }

    public void run() {
      boolean firstTime = true;
      while (!stop || firstTime) {
        firstTime = false;
        for (K k : objects.keySet()) {
          V ob = map.get(k);
          ++accessCount;
          if (ob == null) {
            ++nullCount;
          } else if (!getKey(ob).equals(k)) {
            ++errorCount;
          }
          if (stop) {
            break;
          }
        }
      }
      synchronized (this) {
        done = true;
        notifyAll();
      }
    }

    public synchronized boolean isDone() {
      return done;
    }
  }

  public void testConcurrentPuts() throws InterruptedException {
    final Map<K, V> map = newTestMap(10, null);
    if (!(map instanceof ConcurrentMap)) {
      return;
    }

    final HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);

    ArrayList<ConcurrentPutAccessor> accessors = new ArrayList<ConcurrentPutAccessor>(5);
    for (int i = 0; i < 5; ++i) {
      accessors.add(new ConcurrentPutAccessor(objects, map));
    }

    ArrayList<ConcurrentPutMutator> mutators = new ArrayList<ConcurrentPutMutator>(5);
    for (int i = 0; i < 5; ++i) {
      mutators.add(new ConcurrentPutMutator(objects, map));
    }

    for (int i = 0; i < Math.max(accessors.size(), mutators.size()); ++i) {
      if (i < accessors.size()) {
        accessors.get(i).start();
      }
      if (i < mutators.size()) {
        mutators.get(i).start();
      }
    }

    for (ConcurrentPutMutator m : mutators) {
      synchronized (m) {
        while (!m.isDone()) {
          m.wait();
        }
      }
    }

    for (ConcurrentPutAccessor a : accessors) {
      a.stop = true;
    }

    for (ConcurrentPutAccessor a : accessors) {
      synchronized (a) {
        while (!a.isDone()) {
          a.wait();
        }
      }
    }

    for (ConcurrentPutAccessor a : accessors) {
      log.info(
          "testConcurrentPuts: accessor had "
              + a.accessCount
              + " accesses, "
              + a.nullCount
              + " nulls, and "
              + a.errorCount
              + " errors");
      assertTrue(a.accessCount > 0);
      assertTrue(a.errorCount == 0);
    }

    assertMapsEqual(map, objects);
  }

  public void testClear() {
    final HashMap<K, V> objects = generateUniqueRandomHashMap(SIZE, MIN_KEY, MAX_KEY);
    final Map<K, V> map = newTestMap(objects.size(), null);
    for (Map.Entry<K, V> e : objects.entrySet()) {
      map.put(e.getKey(), e.getValue());
    }
    assertMapsEqual(map, objects);
    assertTrue(mapValuesIdentical(map, objects));
    assertConsistency(map);
    for (int ci = 0; ci < 3; ++ci) {
      map.clear();
      assertMapsEqual(map, Collections.emptyMap());
      assertTrue(mapValuesIdentical(map, Collections.emptyMap()));
      assertConsistency(map);
    }
  }
}

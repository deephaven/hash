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

public class KeyedObjectIntrusiveChainedHash<K, V> implements Iterable<V> {

  protected final IntrusiveChainedHashAdapter<V> adapter;
  protected final KeyedObjectKey<K, V> keyDef;
  protected final float loadFactor;
  protected final boolean rehashEnabled;

  protected V[] buckets;
  protected volatile int size;
  protected int capacity; // how large size can get before we rehash()

  public KeyedObjectIntrusiveChainedHash(
      IntrusiveChainedHashAdapter<V> adapter, KeyedObjectKey<K, V> keyDef) {
    this(11, adapter, keyDef);
  }

  public KeyedObjectIntrusiveChainedHash(
      int initialCapacity, IntrusiveChainedHashAdapter<V> adapter, KeyedObjectKey<K, V> keyDef) {
    this(initialCapacity, 0.5F, adapter, keyDef);
  }

  public KeyedObjectIntrusiveChainedHash(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedObjectKey<K, V> keyDef) {
    this(initialCapacity, loadFactor, adapter, keyDef, true);
  }

  public KeyedObjectIntrusiveChainedHash(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedObjectKey<K, V> keyDef,
      boolean rehashEnabled) {
    this.adapter = adapter;
    this.keyDef = keyDef;
    this.loadFactor = loadFactor;
    this.rehashEnabled = rehashEnabled;
    this.size = 0;
    this.capacity = initialCapacity;
    this.buckets = (V[]) new Object[Math.max((int) (initialCapacity * loadFactor), 1)];
  }

  // -----------------------------------------------------------------------------------------------------

  public int size() {
    return size;
  }

  public int capacity() {
    return capacity;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public V get(Object key) {
    V v;
    int b = (keyDef.hashKey((K) key) & 0x7FFFFFFF) % buckets.length;
    for (v = buckets[b]; v != null; v = adapter.getNext(v)) {
      if (keyDef.equalKey((K) key, v)) {
        break;
      }
    }
    return v;
  }

  public V add(V value) {
    K key = keyDef.getKey(value);
    int b = (keyDef.hashKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p = null, previousValue = null;
    if (v == null) {
      buckets[b] = value;
    } else {
      do {
        if (keyDef.equalKey(key, v)) {
          previousValue = v;
          adapter.setNext(value, adapter.getNext(v));
          adapter.setNext(v, null);
          if (p == null) {
            buckets[b] = value;
          } else {
            adapter.setNext(p, value);
          }
          // no size change - we replaced an existing value
          break;
        }
        p = v;
      } while ((v = adapter.getNext(v)) != null);
      if (v == null) {
        adapter.setNext(value, null);
        adapter.setNext(p, value);
      }
    }
    if (previousValue == null) {
      if (++size > capacity && rehashEnabled) {
        rehash();
      }
    }
    return previousValue;
  }

  public V addIfAbsent(V value) {
    K key = keyDef.getKey(value);
    int b = (keyDef.hashKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p = null, previousValue = null;
    if (v == null) {
      buckets[b] = value;
    } else {
      do {
        if (keyDef.equalKey(key, v)) {
          // no size change - there was already an existing value
          previousValue = v;
          break;
        }
        p = v;
      } while ((v = adapter.getNext(v)) != null);
      if (v == null) {
        adapter.setNext(value, null);
        adapter.setNext(p, value);
      }
    }
    if (previousValue == null) {
      if (++size > capacity && rehashEnabled) {
        rehash();
      }
    }
    return previousValue;
  }

  private void rehash() {
    V[] newBuckets = (V[]) new Object[buckets.length * 2];
    capacity = (int) (newBuckets.length / loadFactor);

    for (int b = 0; b < buckets.length; ++b) {
      for (V v = buckets[b], n; v != null; v = n) {
        n = adapter.getNext(v);
        int newB = (keyDef.hashKey(keyDef.getKey(v)) & 0x7FFFFFFF) % newBuckets.length;
        adapter.setNext(v, newBuckets[newB]);
        newBuckets[newB] = v;
      }
    }
    buckets = newBuckets;
  }

  public boolean removeValue(V value) {
    return removeKey(keyDef.getKey(value)) != null;
  }

  public V removeKey(K key) {
    int b = (keyDef.hashKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p = null;
    if (v != null) {
      do {
        if (keyDef.equalKey(key, v)) {
          if (p == null) {
            buckets[b] = adapter.getNext(v);
          } else {
            adapter.setNext(p, adapter.getNext(v));
          }
          adapter.setNext(v, null);
          size--;
          return v;
        }
        p = v;
      } while ((v = adapter.getNext(v)) != null);
    }
    return null;
  }

  public void clear() {
    for (int b = 0; b < buckets.length; ++b) {
      for (V v = buckets[b], n; v != null; v = n) {
        n = adapter.getNext(v);
        adapter.setNext(v, null);
        size--;
      }
      buckets[b] = null;
    }
    if (size != 0) {
      throw new IllegalStateException("hash size inconsistent: after clear(), size = " + size);
    }
  }

  /**
   * Determines if this hash is equal to a map, as specified in the interface java.util.Map: any two
   * maps must be considered equal if they contain the same set of mappings, regardless of
   * implementation.
   *
   * @param other the other object (presumably a Map)
   * @return true if the objects are equal
   */
  public boolean mapEquals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Map)) {
      return false;
    }
    Map other_map = (Map) other;
    if (other_map.size() != size()) {
      return false;
    }
    for (int b = 0; b < buckets.length; ++b) {
      for (V v = buckets[b]; v != null; v = adapter.getNext(v)) {
        if (!v.equals(other_map.get(keyDef.getKey(v)))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns a hash code for the entire hash, as specified in the interface java.util.Map: the hash
   * code of a map is the sum of the hash codes of its entries, and the hash code of an entry is the
   * bitwise-xor of the hashCodes of the entry's key and value.
   *
   * @return the hash code.
   */
  public int mapHashCode() {
    int hashCode = 0;
    for (int b = 0; b < buckets.length; ++b) {
      for (V v = buckets[b]; v != null; v = adapter.getNext(v)) {
        hashCode += keyDef.getKey(v).hashCode() ^ v.hashCode();
      }
    }
    return hashCode;
  }

  /**
   * Determines if this hash is equal to a set, as specified in the interface java.util.Set: any two
   * sets must be considered equal if they have the same size and contain the same elements,
   * regardless of implementation.
   *
   * @param other the other object (presumably a Set)
   * @return true if the objects are equal
   */
  public boolean setEquals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Set)) {
      return false;
    }
    Set other_set = (Set) other;
    if (other_set.size() != size()) {
      return false;
    }
    for (int b = 0; b < buckets.length; ++b) {
      for (V v = buckets[b]; v != null; v = adapter.getNext(v)) {
        if (!other_set.contains(v)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns a hash code for the entire set, as specified in the interface java.util.Set: the hash
   * code of a set is the sum of the hash codes of its entries.
   *
   * @return the hash code.
   */
  public int setHashCode() {
    int hashCode = 0;
    for (int b = 0; b < buckets.length; ++b) {
      for (V v = buckets[b]; v != null; v = adapter.getNext(v)) {
        hashCode += v.hashCode();
      }
    }
    return hashCode;
  }

  // ------------------------------------------------------------------------------------------------------------
  // Iteration support
  // ------------------------------------------------------------------------------------------------------------

  @Override
  public Iterator<V> iterator() {
    return new ValueIterator();
  }

  /** An Entry class for IntObject maps. */
  private class Entry implements Map.Entry<K, V> {
    private V value;

    public Entry(V v) {
      value = v;
    }

    public K getKey() {
      return keyDef.getKey(value);
    }

    public V getValue() {
      return value;
    }

    public boolean equals(final Object other) {
      return other instanceof Map.Entry
          && ((Map.Entry) other).getKey().equals(keyDef.getKey(value))
          && ((Map.Entry) other).getValue().equals(value);
    }

    public int hashCode() {
      // NOTE: this conforms to the specification of java.util.Map.Entry.hashCode()
      return keyDef.getKey(value).hashCode() ^ value.hashCode();
    }

    public V setValue(V v) {
      // sorry, can't do this one...
      throw new UnsupportedOperationException();
    }
  }

  /*
   ** A KeySet view for IntObject maps.
   */
  private class KeySet extends AbstractSet<K> {
    public Iterator<K> iterator() {
      return new KeyIterator();
    }

    public boolean contains(Object o) {
      return KeyedObjectIntrusiveChainedHash.this.get((K) o) != null;
    }

    public boolean remove(Object o) {
      return KeyedObjectIntrusiveChainedHash.this.removeKey((K) o) != null;
    }

    public int size() {
      return KeyedObjectIntrusiveChainedHash.this.size();
    }

    public void clear() {
      KeyedObjectIntrusiveChainedHash.this.clear();
    }
  }

  /*
   ** A Value collection view for IntObject maps.
   */
  private class ValueCollection extends AbstractCollection<V> {
    public Iterator<V> iterator() {
      return new ValueIterator();
    }

    public boolean contains(Object o) {
      return KeyedObjectIntrusiveChainedHash.this.get(keyDef.getKey((V) o)) != null;
    }

    public boolean remove(Object o) {
      return KeyedObjectIntrusiveChainedHash.this.removeValue((V) o);
    }

    public int size() {
      return KeyedObjectIntrusiveChainedHash.this.size();
    }

    public void clear() {
      KeyedObjectIntrusiveChainedHash.this.clear();
    }
  }

  /*
   ** An EntrySet view for KeyedObjectHashes.
   */
  private class EntrySet extends AbstractSet<Map.Entry<K, V>> {
    public Iterator<Map.Entry<K, V>> iterator() {
      return new EntryIterator();
    }

    public boolean add(Map.Entry<K, V> entry) {
      return KeyedObjectIntrusiveChainedHash.this.add(entry.getValue()) == null;
    }

    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry entry = (Map.Entry) o;
      K key = (K) entry.getKey();
      V value = get(key);
      Object otherValue = entry.getValue();
      return (value == otherValue) || (value != null && value.equals(entry.getValue()));
    }

    public boolean remove(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry entry = (Map.Entry) o;
      return KeyedObjectIntrusiveChainedHash.this.removeKey((K) entry.getKey()) != null;
    }

    public int size() {
      return KeyedObjectIntrusiveChainedHash.this.size();
    }

    public void clear() {
      KeyedObjectIntrusiveChainedHash.this.clear();
    }
  }

  private class BaseIterator {
    // The snapshotted bucket array
    private V[] bs;

    // The bucket we are currently scanning
    private int b;

    // The last value we returned
    private V lastValue;

    // If we remove the current value, then the iterator state points to the *next* value;
    boolean removed;

    /** The constructor just initializes the position to zero. */
    BaseIterator() {
      this.bs = KeyedObjectIntrusiveChainedHash.this.buckets;
      this.b = -1;
      this.lastValue = null;
    }

    /** We have a next element if there is a filled slot at any position >= p. */
    public boolean hasNext() {
      if (removed) {
        return lastValue != null;
      }
      if (lastValue == null || adapter.getNext(lastValue) == null) {
        // first call, or current bucket exhausted
        for (int bi = b + 1; bi < bs.length; ++bi) {
          if (bs[bi] != null) {
            return true;
          }
        }
      } else {
        return true;
      }
      return false;
    }

    private V scan() {
      if (removed) {
        removed = false;
        return lastValue;
      }

      V n;
      if (lastValue == null || (n = adapter.getNext(lastValue)) == null) {
        // first call, or current bucket exhausted
        for (int bi = ++b; bi < bs.length; ++bi) {
          if (bs[bi] != null) {
            lastValue = bs[bi];
            b = bi;
            return lastValue;
          }
        }
      } else {
        lastValue = n;
        return lastValue;
      }
      return lastValue = null;
    }

    protected V scanOrThrow() {
      V v = scan();
      if (v == null) {
        throw new NoSuchElementException("IntrusiveChainedHash Iterator");
      }
      return v;
    }

    public void remove() {
      V v = lastValue;
      lastValue = scan();
      removed = true;
      KeyedObjectIntrusiveChainedHash.this.removeValue(v);
    }
  }

  private class EntryIterator extends BaseIterator implements Iterator<Map.Entry<K, V>> {
    public Map.Entry<K, V> next() {
      return new Entry(super.scanOrThrow());
    }
  }

  private class KeyIterator extends BaseIterator implements Iterator<K> {
    public K next() {
      return keyDef.getKey(super.scanOrThrow());
    }
  }

  private class ValueIterator extends BaseIterator implements Iterator<V> {
    public V next() {
      return super.scanOrThrow();
    }
  }

  public Set<K> keySet() {
    return new KeySet();
  }

  public Collection<V> values() {
    return new ValueCollection();
  }

  public Set<Map.Entry<K, V>> entrySet() {
    return new EntrySet();
  }
}

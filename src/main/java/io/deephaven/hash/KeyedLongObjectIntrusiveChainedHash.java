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

public class KeyedLongObjectIntrusiveChainedHash<V> extends KeyedObjectIntrusiveChainedHash<Long, V>
    implements Iterable<V> {

  /** The key definition for this instance. */
  protected KeyedLongObjectKey<V> longKeyDef;

  /** Constructors */
  public KeyedLongObjectIntrusiveChainedHash(
      IntrusiveChainedHashAdapter<V> adapter, KeyedLongObjectKey<V> keyDef) {
    super(adapter, keyDef);
    this.longKeyDef = keyDef;
  }

  public KeyedLongObjectIntrusiveChainedHash(
      int initialCapacity, IntrusiveChainedHashAdapter<V> adapter, KeyedLongObjectKey<V> keyDef) {
    super(initialCapacity, adapter, keyDef);
    this.longKeyDef = keyDef;
  }

  public KeyedLongObjectIntrusiveChainedHash(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedLongObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, adapter, keyDef);
    this.longKeyDef = keyDef;
  }

  public KeyedLongObjectIntrusiveChainedHash(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedLongObjectKey<V> keyDef,
      boolean rehashEnabled) {
    super(initialCapacity, loadFactor, adapter, keyDef, rehashEnabled);
    this.longKeyDef = keyDef;
  }

  // -----------------------------------------------------------------------------------------------------

  public boolean containsKey(long key) {
    return get(key) != null;
  }

  public V get(long key) {
    V v;
    int b = (longKeyDef.hashLongKey(key) & 0x7FFFFFFF) % buckets.length;
    for (v = buckets[b]; v != null; v = adapter.getNext(v)) {
      if (longKeyDef.equalLongKey(key, v)) {
        break;
      }
    }
    return v;
  }

  public V put(long key, V value) {
    if (!longKeyDef.equalLongKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(value));
    }
    return add(value);
  }

  public V add(V value) {
    long key = longKeyDef.getLongKey(value);
    int b = (longKeyDef.hashLongKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p = null, previousValue = null;
    if (v == null) {
      buckets[b] = value;
    } else {
      do {
        if (longKeyDef.equalLongKey(key, v)) {
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

  private void rehash() {
    V[] newBuckets = (V[]) new Object[buckets.length * 2];
    capacity = (int) (newBuckets.length / loadFactor);

    for (int b = 0; b < buckets.length; ++b) {
      for (V v = buckets[b], n; v != null; v = n) {
        n = adapter.getNext(v);
        int newB =
            (longKeyDef.hashLongKey(longKeyDef.getLongKey(v)) & 0x7FFFFFFF) % newBuckets.length;
        adapter.setNext(v, newBuckets[newB]);
        newBuckets[newB] = v;
      }
    }
    buckets = newBuckets;
  }

  public boolean removeValue(V value) {
    return removeKey(longKeyDef.getLongKey(value)) != null;
  }

  public V remove(long key) {
    return removeKey(key);
  }

  public V removeKey(long key) {
    int b = (longKeyDef.hashLongKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p = null;
    if (v != null) {
      do {
        if (longKeyDef.equalLongKey(key, v)) {
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

  // ------------------------------------------------------------------------------------------------

  public <T> V putIfAbsent(
      long key, KeyedLongObjectHash.ValueFactoryIT<V, T> factory, int extra, T extra2) {
    int b = (longKeyDef.hashLongKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p, result;
    if (v == null) {
      buckets[b] = result = factory.newValue(key, extra, extra2);
    } else {
      do {
        if (longKeyDef.equalLongKey(key, v)) {
          return v;
        }
        p = v;
      } while ((v = adapter.getNext(v)) != null);
      result = factory.newValue(key, extra, extra2);
      adapter.setNext(result, null);
      adapter.setNext(p, result);
    }
    if (++size > capacity && rehashEnabled) {
      rehash();
    }
    return result;
  }
}

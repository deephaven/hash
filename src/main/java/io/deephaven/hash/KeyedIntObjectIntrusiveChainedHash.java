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

public class KeyedIntObjectIntrusiveChainedHash<V>
    extends KeyedObjectIntrusiveChainedHash<Integer, V> implements Iterable<V> {

  /** The key definition for this instance. */
  protected KeyedIntObjectKey<V> intKeyDef;

  /** Constructors */
  public KeyedIntObjectIntrusiveChainedHash(
      IntrusiveChainedHashAdapter<V> adapter, KeyedIntObjectKey<V> keyDef) {
    super(adapter, keyDef);
    this.intKeyDef = keyDef;
  }

  public KeyedIntObjectIntrusiveChainedHash(
      int initialCapacity, IntrusiveChainedHashAdapter<V> adapter, KeyedIntObjectKey<V> keyDef) {
    super(initialCapacity, adapter, keyDef);
    this.intKeyDef = keyDef;
  }

  public KeyedIntObjectIntrusiveChainedHash(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedIntObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, adapter, keyDef);
    this.intKeyDef = keyDef;
  }

  public KeyedIntObjectIntrusiveChainedHash(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedIntObjectKey<V> keyDef,
      boolean rehashEnabled) {
    super(initialCapacity, loadFactor, adapter, keyDef, rehashEnabled);
    this.intKeyDef = keyDef;
  }

  // -----------------------------------------------------------------------------------------------------

  public boolean containsKey(int key) {
    return get(key) != null;
  }

  public V get(int key) {
    V v;
    int b = (intKeyDef.hashIntKey(key) & 0x7FFFFFFF) % buckets.length;
    for (v = buckets[b]; v != null; v = adapter.getNext(v)) {
      if (intKeyDef.equalIntKey(key, v)) {
        break;
      }
    }
    return v;
  }

  public V put(int key, V value) {
    if (!intKeyDef.equalIntKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(value));
    }
    return add(value);
  }

  public V add(V value) {
    int key = intKeyDef.getIntKey(value);
    int b = (intKeyDef.hashIntKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p = null, previousValue = null;
    if (v == null) {
      buckets[b] = value;
    } else {
      do {
        if (intKeyDef.equalIntKey(key, v)) {
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
        int newB = (intKeyDef.hashIntKey(intKeyDef.getIntKey(v)) & 0x7FFFFFFF) % newBuckets.length;
        adapter.setNext(v, newBuckets[newB]);
        newBuckets[newB] = v;
      }
    }
    buckets = newBuckets;
  }

  public boolean removeValue(V value) {
    return removeKey(intKeyDef.getIntKey(value)) != null;
  }

  public V remove(int key) {
    return removeKey(key);
  }

  public V removeKey(int key) {
    int b = (intKeyDef.hashIntKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p = null;
    if (v != null) {
      do {
        if (intKeyDef.equalIntKey(key, v)) {
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
      int key, KeyedIntObjectHash.ValueFactoryIT<V, T> factory, int extra, T extra2) {
    int b = (intKeyDef.hashIntKey(key) & 0x7FFFFFFF) % buckets.length;
    V v = buckets[b], p, result;
    if (v == null) {
      buckets[b] = result = factory.newValue(key, extra, extra2);
    } else {
      do {
        if (intKeyDef.equalIntKey(key, v)) {
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

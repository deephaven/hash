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

import java.util.Map;

public class KeyedLongObjectIntrusiveChainedHashMap<V>
    extends KeyedLongObjectIntrusiveChainedHash<V> implements Map<Long, V> {

  public KeyedLongObjectIntrusiveChainedHashMap(
      IntrusiveChainedHashAdapter<V> adapter, KeyedLongObjectKey<V> keyDef) {
    super(adapter, keyDef);
  }

  public KeyedLongObjectIntrusiveChainedHashMap(
      int initialCapacity, IntrusiveChainedHashAdapter<V> adapter, KeyedLongObjectKey<V> keyDef) {
    super(initialCapacity, adapter, keyDef);
  }

  public KeyedLongObjectIntrusiveChainedHashMap(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedLongObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, adapter, keyDef, true);
  }

  public KeyedLongObjectIntrusiveChainedHashMap(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedLongObjectKey<V> keyDef,
      boolean rehashEnabled) {
    super(initialCapacity, loadFactor, adapter, keyDef, rehashEnabled);
  }

  @Override
  public boolean equals(Object other) {
    return super.mapEquals(other);
  }

  @Override
  public int hashCode() {
    return super.mapHashCode();
  }

  @Override
  public boolean containsKey(Object key) {
    return containsKey((long) key);
  }

  @Override
  public boolean containsValue(Object value) {
    V v = get(longKeyDef.getLongKey((V) value));
    return v == value;
  }

  @Override
  public V put(Long key, V value) {
    if (!keyDef.equalKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
    }
    return add(value);
  }

  @Override
  public V remove(Object key) {
    return key instanceof Long ? removeKey((long) (Long) key) : null;
  }

  @Override
  public void putAll(Map<? extends Long, ? extends V> t) {
    // TODO: it's impossible to know which kind of iteration is better: entrySet() or keySet()+get()
    for (Long key : t.keySet()) {
      V value = t.get(key);
      if (!keyDef.equalKey(key, value)) {
        throw new IllegalArgumentException(
            "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
      }
      add(value);
    }
  }
}

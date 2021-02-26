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

public class KeyedIntObjectIntrusiveChainedHashMap<V> extends KeyedIntObjectIntrusiveChainedHash<V>
    implements Map<Integer, V> {

  public KeyedIntObjectIntrusiveChainedHashMap(
      IntrusiveChainedHashAdapter<V> adapter, KeyedIntObjectKey<V> keyDef) {
    super(adapter, keyDef);
  }

  public KeyedIntObjectIntrusiveChainedHashMap(
      int initialCapacity, IntrusiveChainedHashAdapter<V> adapter, KeyedIntObjectKey<V> keyDef) {
    super(initialCapacity, adapter, keyDef);
  }

  public KeyedIntObjectIntrusiveChainedHashMap(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedIntObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, adapter, keyDef, true);
  }

  public KeyedIntObjectIntrusiveChainedHashMap(
      int initialCapacity,
      float loadFactor,
      IntrusiveChainedHashAdapter<V> adapter,
      KeyedIntObjectKey<V> keyDef,
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
    return containsKey((int) key);
  }

  @Override
  public boolean containsValue(Object value) {
    V v = get(intKeyDef.getIntKey((V) value));
    return v == value;
  }

  @Override
  public V put(Integer key, V value) {
    if (!keyDef.equalKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
    }
    return add(value);
  }

  @Override
  public V remove(Object key) {
    return key instanceof Integer ? removeKey((int) (Integer) key) : null;
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends V> t) {
    // TODO: it's impossible to know which kind of iteration is better: entrySet() or keySet()+get()
    for (Integer key : t.keySet()) {
      V value = t.get(key);
      if (!keyDef.equalKey(key, value)) {
        throw new IllegalArgumentException(
            "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
      }
      add(value);
    }
  }
}

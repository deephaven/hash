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

import java.util.concurrent.ConcurrentMap;

public class KeyedIntObjectHashMap<V> extends KeyedIntObjectHash<V>
    implements ConcurrentMap<Integer, V>, IndexableMap<Integer, V> {

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with the default capacity and load factor.
   */
  public KeyedIntObjectHashMap(KeyedIntObjectKey<V> keyDef) {
    super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedIntObjectHashMap</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public KeyedIntObjectHashMap(int initialCapacity, KeyedIntObjectKey<V> keyDef) {
    super(initialCapacity, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedIntObjectHashMap</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public KeyedIntObjectHashMap(int initialCapacity, float loadFactor, KeyedIntObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, keyDef);
  }

  /**
   * Determines if this map is equal to another map.
   *
   * @return true if the objects are equal
   */
  public boolean equals(Object other) {
    return super.mapEquals(other);
  }

  /**
   * Returns a hash code for the map as a whole
   *
   * @return the hash code.
   */
  public int hashCode() {
    return super.mapHashCode();
  }

  /**
   * Remove the mapping for the given key from the map.
   *
   * @param key the key of the mapping to be removed
   * @return the value of the removed mapping, or null if no mapping was present
   */
  public V remove(Object key) {
    int k = (Integer) key;
    return super.removeKey(k);
  }

  /**
   * Remove the mapping for the given key from the map.
   *
   * @param key the key of the mapping to be removed
   * @return the value of the removed mapping, or null if no mapping was present
   */
  public V remove(int key) {
    return super.removeKey(key);
  }
}

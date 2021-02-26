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

public class KeyedObjectHashMap<K, V> extends KeyedObjectHash<K, V>
    implements ConcurrentMap<K, V>, IndexableMap<K, V> {

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with the default capacity and load factor.
   */
  public KeyedObjectHashMap(KeyedObjectKey<K, V> keyDef) {
    super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedObjectHashMap</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public KeyedObjectHashMap(int initialCapacity, KeyedObjectKey<K, V> keyDef) {
    super(initialCapacity, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedObjectHashMap</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public KeyedObjectHashMap(int initialCapacity, float loadFactor, KeyedObjectKey<K, V> keyDef) {
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
    return super.removeKey((K) key);
  }
}

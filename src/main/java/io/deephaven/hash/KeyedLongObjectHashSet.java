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

import java.util.Set;

public class KeyedLongObjectHashSet<K, V> extends KeyedLongObjectHash<V> implements Set<V> {

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with the default capacity and load factor.
   */
  public KeyedLongObjectHashSet(KeyedLongObjectKey<V> keyDef) {
    super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedLongObjectHashSet</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public KeyedLongObjectHashSet(int initialCapacity, KeyedLongObjectKey<V> keyDef) {
    super(initialCapacity, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedLongObjectHashSet</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public KeyedLongObjectHashSet(
      int initialCapacity, float loadFactor, KeyedLongObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, keyDef);
  }

  /**
   * Determines if this set is equal to another set.
   *
   * @param other the other object (presumably a Set)
   * @return true if the objects are equal
   */
  public boolean equals(Object other) {
    return super.setEquals(other);
  }

  /**
   * Returns a hash code for the set as a whole.
   *
   * @return the hash code.
   */
  public int hashCode() {
    return super.setHashCode();
  }

  /**
   * Removes an object from the set
   *
   * @param o
   * @return true, if the object was in the set.
   */
  public boolean remove(Object o) {
    return super.removeValue((V) o);
  }
}

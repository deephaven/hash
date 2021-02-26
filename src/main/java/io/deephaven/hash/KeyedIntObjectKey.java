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

public interface KeyedIntObjectKey<V> extends KeyedObjectKey<Integer, V> {
  /**
   * Returns the key of an object as a primitive int.
   *
   * @param v the object
   * @return its key
   */
  public int getIntKey(V v);

  /**
   * Returns the hash code of a primitive int key.
   *
   * @param k the key
   * @return the hash code of the object's key
   */
  public int hashIntKey(int k);

  /**
   * Compare a primitive int key against the key of an object.
   *
   * @param k the key
   * @param v the object
   * @return true, if the given key is equal to the object's key
   */
  public boolean equalIntKey(int k, V v);

  // implementation which unboxes boxed integers
  public abstract static class Lax<V> implements KeyedIntObjectKey<V> {
    public final Integer getKey(V v) {
      return getIntKey(v);
    }

    public final int hashKey(Integer integer) {
      return hashIntKey(integer);
    }

    public final boolean equalKey(Integer integer, V v) {
      return equalIntKey(integer, v);
    }
  }

  // implementation which complains if boxed integers are used
  public abstract static class Strict<V> implements KeyedIntObjectKey<V> {
    public final Integer getKey(V v) {
      throw new IllegalArgumentException("Please don't use boxed integers");
    }

    public final int hashKey(Integer integer) {
      throw new IllegalArgumentException("Please don't use boxed integers");
    }

    public final boolean equalKey(Integer integer, V v) {
      throw new IllegalArgumentException("Please don't use boxed integers");
    }
  }

  public abstract static class BasicStrict<V> extends Strict<V> {
    public int hashIntKey(final int k) {
      return k;
    }

    public boolean equalIntKey(final int k, final V v) {
      return k == getIntKey(v);
    }
  }
}

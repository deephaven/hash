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

public interface KeyedLongObjectKey<V> extends KeyedObjectKey<Long, V> {
  /**
   * Returns the key of an object as a primitive long.
   *
   * @param v the object
   * @return its key
   */
  public long getLongKey(V v);

  /**
   * Returns the hash code of a primitive long key.
   *
   * @param k the key
   * @return the hash code of the object's key
   */
  public int hashLongKey(long k);

  /**
   * Compare a primitive long key against the key of an object.
   *
   * @param k the key
   * @param v the object
   * @return true, if the given key is equal to the object's key
   */
  public boolean equalLongKey(long k, V v);

  // implementation which unboxes boxed long
  public abstract static class Lax<V> implements KeyedLongObjectKey<V> {
    public final Long getKey(V v) {
      return getLongKey(v);
    }

    public final int hashKey(Long k) {
      return hashLongKey(k);
    }

    public final boolean equalKey(Long k, V v) {
      return equalLongKey(k, v);
    }
  }

  // implementation which complains if boxed long are used
  public abstract static class Strict<V> implements KeyedLongObjectKey<V> {
    public final Long getKey(V v) {
      throw new IllegalArgumentException("Please don't use boxed longs");
    }

    public final int hashKey(Long k) {
      throw new IllegalArgumentException("Please don't use boxed longs");
    }

    public final boolean equalKey(Long k, V v) {
      throw new IllegalArgumentException("Please don't use boxed longs");
    }
  }

  public abstract static class BasicLax<V> extends Lax<V> {
    public int hashLongKey(final long k) {
      return (int) (k ^ (k >>> 32));
    }

    public boolean equalLongKey(final long k, final V v) {
      return k == getLongKey(v);
    }
  }

  public abstract static class BasicStrict<V> extends Strict<V> {
    public int hashLongKey(final long k) {
      return (int) (k ^ (k >>> 32));
    }

    public boolean equalLongKey(final long k, final V v) {
      return k == getLongKey(v);
    }
  }
}

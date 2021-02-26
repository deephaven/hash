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

public interface KeyedDoubleObjectKey<V> extends KeyedObjectKey<Double, V> {
  /**
   * Returns the key of an object as a primitive double.
   *
   * @param v the object
   * @return its key
   */
  public double getDoubleKey(V v);

  /**
   * Returns the hash code of a primitive double key.
   *
   * @param k the key
   * @return the hash code of the object's key
   */
  public int hashDoubleKey(double k);

  /**
   * Compare a primitive double key against the key of an object.
   *
   * @param k the key
   * @param v the object
   * @return true, if the given key is equal to the object's key
   */
  public boolean equalDoubleKey(double k, V v);

  // implementation which unboxes boxed doubles
  public abstract static class Lax<V> implements KeyedDoubleObjectKey<V> {
    public final Double getKey(V v) {
      return getDoubleKey(v);
    }

    public final int hashKey(Double k) {
      return hashDoubleKey(k);
    }

    public final boolean equalKey(Double k, V v) {
      return equalDoubleKey(k, v);
    }
  }

  // implementation which complains if boxed doubles are used
  public abstract static class Strict<V> implements KeyedDoubleObjectKey<V> {
    public final Double getKey(V v) {
      throw new IllegalArgumentException("Please don't use boxed doubles");
    }

    public final int hashKey(Double k) {
      throw new IllegalArgumentException("Please don't use boxed doubles");
    }

    public final boolean equalKey(Double k, V v) {
      throw new IllegalArgumentException("Please don't use boxed doubles");
    }
  }
}

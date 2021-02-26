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

import java.util.Objects;
import java.util.function.Function;

/** This interface adapts a class for use with a KeyedObjectHash. */
@SuppressWarnings("unused")
public interface KeyedObjectKey<K, V> {
  /**
   * Returns the key of an object.
   *
   * @param v the object
   * @return its key
   */
  K getKey(V v);

  /**
   * Returns the hash code of a key.
   *
   * @param k the key
   * @return the hash code of the object's key
   */
  int hashKey(K k);

  /**
   * Compare a key against the key of an object.
   *
   * @param k the key
   * @param v the object
   * @return true, if the given key is equal to the object's key
   */
  boolean equalKey(K k, V v);

  abstract class Basic<K, V> implements KeyedObjectKey<K, V> {
    @Override
    public boolean equalKey(final K k, final V v) {
      return k.equals(getKey(v));
    }

    @Override
    public int hashKey(final K k) {
      return k.hashCode();
    }
  }

  class BasicAdapter<K, V> extends Basic<K, V> {

    private final Function<V, K> keyFunction;

    public BasicAdapter(final Function<V, K> keyFunction) {
      this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction");
    }

    @Override
    public K getKey(final V v) {
      return keyFunction.apply(v);
    }
  }

  abstract class NullSafeBasic<K, V> implements KeyedObjectKey<K, V> {
    @Override
    public boolean equalKey(final K k, final V v) {
      return k == null ? getKey(v) == null : k.equals(getKey(v));
    }

    @Override
    public int hashKey(final K k) {
      return k == null ? 0 : k.hashCode();
    }
  }

  class NullSafeBasicAdapter<K, V> extends NullSafeBasic<K, V> {

    private final Function<V, K> keyFunction;

    public NullSafeBasicAdapter(final Function<V, K> keyFunction) {
      this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction");
    }

    @Override
    public K getKey(final V v) {
      return keyFunction.apply(v);
    }
  }

  abstract class Exact<K, V> implements KeyedObjectKey<K, V> {
    @Override
    public boolean equalKey(final K k, final V v) {
      return k == getKey(v);
    }

    @Override
    public int hashKey(final K k) {
      return k.hashCode();
    }
  }

  class ExactAdapter<K, V> extends Exact<K, V> {

    private final Function<V, K> keyFunction;

    public ExactAdapter(final Function<V, K> keyFunction) {
      this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction");
    }

    @Override
    public K getKey(final V v) {
      return keyFunction.apply(v);
    }
  }

  abstract class NullSafeExact<K, V> implements KeyedObjectKey<K, V> {
    @Override
    public boolean equalKey(final K k, final V v) {
      return k == getKey(v);
    }

    @Override
    public int hashKey(final K k) {
      return k == null ? 0 : k.hashCode();
    }
  }

  class NullSafeExactAdapter<K, V> extends NullSafeExact<K, V> {

    private final Function<V, K> keyFunction;

    public NullSafeExactAdapter(final Function<V, K> keyFunction) {
      this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction");
    }

    @Override
    public K getKey(final V v) {
      return keyFunction.apply(v);
    }
  }
}

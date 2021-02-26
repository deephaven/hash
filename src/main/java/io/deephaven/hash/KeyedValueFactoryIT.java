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

/**
 * If values can be constructed from just a key, an int and an object, this interface can be used
 * with putIfAbsent(K, ValueFactoryIT, int, T) to implement an atomic find-or-add operation.
 */
public interface KeyedValueFactoryIT<K, V, T> {
  V newValue(K key, int extra, T extra2);
}

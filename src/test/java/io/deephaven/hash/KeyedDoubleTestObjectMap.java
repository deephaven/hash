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

/** Instantiation of KeyedDoubleObjectHashMap on the test class. */
class KeyedDoubleTestObjectMap extends KeyedDoubleObjectHashMap<KeyedDoubleTestObject> {
  public KeyedDoubleTestObjectMap() {
    super(KeyedDoubleTestObject.keyDef);
  }

  public KeyedDoubleTestObjectMap(int initialCapacity) {
    super(initialCapacity, KeyedDoubleTestObject.keyDef);
  }

  public KeyedDoubleTestObjectMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor, KeyedDoubleTestObject.keyDef);
  }

  public final double getId(KeyedDoubleTestObject obj) {
    return obj.getId();
  }
}

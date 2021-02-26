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

/** Instantiation of KeyedLongObjectHashMap on the test class. */
class KeyedLongTestObjectMap extends KeyedLongObjectHashMap<KeyedLongTestObject> {
  public KeyedLongTestObjectMap() {
    super(KeyedLongTestObject.keyDef);
  }

  public KeyedLongTestObjectMap(int initialCapacity) {
    super(initialCapacity, KeyedLongTestObject.keyDef);
  }

  public KeyedLongTestObjectMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor, KeyedLongTestObject.keyDef);
  }

  public final long getId(KeyedLongTestObject obj) {
    return obj.getId();
  }
}

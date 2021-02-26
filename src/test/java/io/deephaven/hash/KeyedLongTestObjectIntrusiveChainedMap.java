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

/** Instantiate a keyed object map on the test object */
class KeyedLongTestObjectIntrusiveChainedMap
    extends KeyedLongObjectIntrusiveChainedHashMap<KeyedLongTestObject> {
  public KeyedLongTestObjectIntrusiveChainedMap() {
    super(KeyedLongTestObject.adapter, KeyedLongTestObject.keyDef);
  }

  public KeyedLongTestObjectIntrusiveChainedMap(int initialCapacity) {
    super(initialCapacity, KeyedLongTestObject.adapter, KeyedLongTestObject.keyDef);
  }

  public KeyedLongTestObjectIntrusiveChainedMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor, KeyedLongTestObject.adapter, KeyedLongTestObject.keyDef);
  }

  public KeyedLongTestObjectIntrusiveChainedMap(
      int initialCapacity, float loadFactor, boolean rehashEnabled) {
    super(
        initialCapacity,
        loadFactor,
        KeyedLongTestObject.adapter,
        KeyedLongTestObject.keyDef,
        rehashEnabled);
  }

  public final long getId(KeyedLongTestObject obj) {
    return obj.getId();
  }
}

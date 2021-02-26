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

/** Insantiate a keyed object map on the test object */
class KeyedTestObjectIntrusiveChainedMap
    extends KeyedObjectIntrusiveChainedHashMap<String, KeyedTestObject> {
  public KeyedTestObjectIntrusiveChainedMap() {
    super(KeyedTestObject.adapter, KeyedTestObject.keyDef);
  }

  public KeyedTestObjectIntrusiveChainedMap(int initialCapacity) {
    super(initialCapacity, KeyedTestObject.adapter, KeyedTestObject.keyDef);
  }

  public KeyedTestObjectIntrusiveChainedMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor, KeyedTestObject.adapter, KeyedTestObject.keyDef);
  }

  public KeyedTestObjectIntrusiveChainedMap(
      int initialCapacity, float loadFactor, boolean rehashEnabled) {
    super(
        initialCapacity,
        loadFactor,
        KeyedTestObject.adapter,
        KeyedTestObject.keyDef,
        rehashEnabled);
  }

  public final String getId(KeyedTestObject obj) {
    return obj.getId();
  }
}

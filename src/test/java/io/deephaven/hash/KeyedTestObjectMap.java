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
class KeyedTestObjectMap extends KeyedObjectHashMap<String, KeyedTestObject> {
  public KeyedTestObjectMap() {
    super(KeyedTestObject.keyDef);
  }

  public KeyedTestObjectMap(int initialCapacity) {
    super(initialCapacity, KeyedTestObject.keyDef);
  }

  public KeyedTestObjectMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor, KeyedTestObject.keyDef);
  }

  public final String getId(KeyedTestObject obj) {
    return obj.getId();
  }
}

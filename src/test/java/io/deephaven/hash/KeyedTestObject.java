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

/** the test object */
class KeyedTestObject {
  private String id;

  public KeyedTestObject(String s) {
    this.id = s;
  }

  public String getId() {
    return id;
  }

  public boolean equals(Object other) {
    return other instanceof KeyedTestObject && id.equals(((KeyedTestObject) other).id);
  }

  public int hashCode() {
    return ~id.hashCode(); // do something different than the standard java object
  }

  public String toString() {
    return "[KeyedTestObject:" + id + "]";
  }

  public static final KeyedObjectKey<String, KeyedTestObject> keyDef =
      new KeyedObjectKey<String, KeyedTestObject>() {
        public String getKey(KeyedTestObject v) {
          return v.getId();
        }

        public int hashKey(String k) {
          return k.hashCode();
        }

        public boolean equalKey(String k, KeyedTestObject v) {
          return v.getId().equals(k);
        }
      };

  // for intrusive chained maps

  private KeyedTestObject next;
  public static final IntrusiveChainedHashAdapter<KeyedTestObject> adapter =
      new IntrusiveChainedHashAdapter<KeyedTestObject>() {
        @Override
        public KeyedTestObject getNext(KeyedTestObject self) {
          return self.next;
        }

        @Override
        public void setNext(KeyedTestObject self, KeyedTestObject next) {
          self.next = next;
        }
      };
}

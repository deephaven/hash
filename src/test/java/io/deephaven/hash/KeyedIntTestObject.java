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

/** Test class. */
class KeyedIntTestObject {
  private int id;

  public KeyedIntTestObject(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public boolean equals(Object other) {
    return other instanceof KeyedIntTestObject && id == ((KeyedIntTestObject) other).id;
  }

  public int hashCode() {
    return ~id; // do something different that gnu.trove.HashFunctions.hash(int)
  }

  public String toString() {
    return "[KeyedIntTestObject:" + id + "]";
  }

  public static final KeyedIntObjectKey<KeyedIntTestObject> keyDef =
      new KeyedIntObjectKey<KeyedIntTestObject>() {
        public Integer getKey(KeyedIntTestObject v) {
          return v.getId();
        }

        public int getIntKey(KeyedIntTestObject v) {
          return v.getId();
        }

        public int hashKey(Integer k) {
          return k;
        }

        public int hashIntKey(int k) {
          return k;
        }

        public boolean equalKey(Integer k, KeyedIntTestObject v) {
          return v.getId() == Integer.valueOf(k);
        }

        public boolean equalIntKey(int k, KeyedIntTestObject v) {
          return v.getId() == k;
        }
      };

  public static final KeyedIntObjectHash.ValueFactory<KeyedIntTestObject> factory =
      new KeyedIntObjectHash.ValueFactory<KeyedIntTestObject>() {
        public KeyedIntTestObject newValue(Integer key) {
          return new KeyedIntTestObject(key);
        }

        public KeyedIntTestObject newValue(int key) {
          return new KeyedIntTestObject(key);
        }
      };

  // for intrusive chained maps

  private KeyedIntTestObject next;

  public static final IntrusiveChainedHashAdapter<KeyedIntTestObject> adapter =
      new IntrusiveChainedHashAdapter<KeyedIntTestObject>() {
        @Override
        public KeyedIntTestObject getNext(KeyedIntTestObject self) {
          return self.next;
        }

        @Override
        public void setNext(KeyedIntTestObject self, KeyedIntTestObject next) {
          self.next = next;
        }
      };
}

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
class KeyedDoubleTestObject {
  private final double id;

  public KeyedDoubleTestObject(double id) {
    this.id = id;
  }

  public double getId() {
    return id;
  }

  public boolean equals(Object other) {
    return other instanceof KeyedDoubleTestObject && id == ((KeyedDoubleTestObject) other).id;
  }

  public int hashCode() {
    return ~Double.hashCode(id); // do something different that gnu.trove.HashFunctions.hash(double)
  }

  public String toString() {
    return "[KeyedDoubleTestObject:" + id + "]";
  }

  public static final KeyedDoubleObjectKey<KeyedDoubleTestObject> keyDef =
      new KeyedDoubleObjectKey<KeyedDoubleTestObject>() {
        public Double getKey(KeyedDoubleTestObject v) {
          return v.getId();
        }

        public double getDoubleKey(KeyedDoubleTestObject v) {
          return v.getId();
        }

        public int hashKey(Double k) {
          return k.hashCode();
        }

        @Override
        public boolean equalKey(Double k, KeyedDoubleTestObject v) {
          return v.getId() == k;
        }

        public int hashDoubleKey(double k) {
          return Double.hashCode(k);
        }

        public boolean equalDoubleKey(double k, KeyedDoubleTestObject v) {
          return v.getId() == k;
        }
      };

  public static final KeyedIntObjectHash.ValueFactory<KeyedDoubleTestObject> factory =
      new KeyedIntObjectHash.ValueFactory<KeyedDoubleTestObject>() {
        public KeyedDoubleTestObject newValue(Integer key) {
          return new KeyedDoubleTestObject(key);
        }

        public KeyedDoubleTestObject newValue(int key) {
          return new KeyedDoubleTestObject(key);
        }
      };

  // for intrusive chained maps

  private KeyedDoubleTestObject next;

  public static final IntrusiveChainedHashAdapter<KeyedDoubleTestObject> adapter =
      new IntrusiveChainedHashAdapter<KeyedDoubleTestObject>() {
        @Override
        public KeyedDoubleTestObject getNext(KeyedDoubleTestObject self) {
          return self.next;
        }

        @Override
        public void setNext(KeyedDoubleTestObject self, KeyedDoubleTestObject next) {
          self.next = next;
        }
      };
}

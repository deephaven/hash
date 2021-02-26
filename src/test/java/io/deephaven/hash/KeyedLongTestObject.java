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
class KeyedLongTestObject {
  private long id;

  public KeyedLongTestObject(long id) {
    this.id = id;
  }

  public long getId() {
    return id;
  }

  public boolean equals(Object other) {
    return other instanceof KeyedLongTestObject && id == ((KeyedLongTestObject) other).id;
  }

  public int hashCode() {
    return (int) (~id ^ (~id >>> 32));
  }

  public String toString() {
    return "[KeyedLongTestObject:" + id + "]";
  }

  public static final KeyedLongObjectKey<KeyedLongTestObject> keyDef =
      new KeyedLongObjectKey<KeyedLongTestObject>() {
        public Long getKey(KeyedLongTestObject v) {
          return v.getId();
        }

        public long getLongKey(KeyedLongTestObject v) {
          return v.getId();
        }

        public int hashKey(Long k) {
          return k.hashCode();
        }

        public int hashLongKey(long k) {
          return (int) (k ^ (k >>> 32));
        } // TODO: Replace with Long.hashCode(long) once we have Java 8 everywhere.

        public boolean equalKey(Long k, KeyedLongTestObject v) {
          return v.getId() == Long.valueOf(k);
        }

        public boolean equalLongKey(long k, KeyedLongTestObject v) {
          return v.getId() == k;
        }
      };

  public static final KeyedLongObjectHash.ValueFactory<KeyedLongTestObject> factory =
      new KeyedLongObjectHash.ValueFactory<KeyedLongTestObject>() {
        public KeyedLongTestObject newValue(Long key) {
          return new KeyedLongTestObject(key);
        }

        public KeyedLongTestObject newValue(long key) {
          return new KeyedLongTestObject(key);
        }
      };

  // for intrusive chained maps

  private KeyedLongTestObject next;

  public static final IntrusiveChainedHashAdapter<KeyedLongTestObject> adapter =
      new IntrusiveChainedHashAdapter<KeyedLongTestObject>() {
        @Override
        public KeyedLongTestObject getNext(KeyedLongTestObject self) {
          return self.next;
        }

        @Override
        public void setNext(KeyedLongTestObject self, KeyedLongTestObject next) {
          self.next = next;
        }
      };
}

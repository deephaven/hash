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

import java.io.Serializable;
import java.util.Objects;

/**
 * This collection implements a hashed set of objects identified by a key; the characteristics of
 * the key are defined by an instance of the KeyedObjectKey interface.
 *
 * <p>This class implements most of both the Set and ConcurrentMap interfaces. It can't fully
 * implement both because of discrepancies in the signatures and/or semantics of the remove(),
 * equals() and hashCode() methods. The subclasses KeyedObjectHashMap and KeyedObjectHashSet fill in
 * the missing bits.
 *
 * <p>In cases where the key to a map is just an attribute of the mapped value, this design wil save
 * memory over an equivalent Map instance, since no extra slot is required to hold a reference to
 * (or in the worst case, a copy of) the key attribute. This can be significant if the key attribute
 * is a primitive int. We also use an open-addressed hash, so Map.Entry instance are also unecessary
 * except for iteration over the entrySet() collection.
 *
 * <p>HOWEVER: note that open-addressed hashing may not be appropriate for tables whose populations
 * grow and shrink dynamically.This is because the markers for deleted slots can lengthen probe
 * sequences. In the worst case, this can lead to O(capacity) performance for get() and put()
 * operations.
 *
 * <p>The implementation is also thread-safe; put() operations are internally synchronized, and
 * get() operations in the presence of concurrent puts will never throw a
 * ConcurrentModificationException. If puts() are performed during an iteration, the results of the
 * iteration are not defined (as in java's ConcurrentHashMap).
 *
 * <p>Just for good measure, this class also implements the IndexableMap interface.
 *
 * <p>I am fairly certain that practical problems with leaving get completely unsynchronized are
 * non-existent, or can be made to be so by the addition of a volatile barrier to enforce the
 * happens-before relationship between writes to the storage[] array and reads from it in get().
 *
 * <p>1. All methods which write to any part the hash are fully synchronized. This means that for
 * the purposes of this analysis, it is sufficient to consider the interaction between a single
 * put() call and a single get() call. 2. The only field which get() reads is storage[], which is
 * initialized and in a consistent state as soon as the object is constructed. It copies the
 * reference to storage[] into a local variable, so it cannot be affected by a rehash. 3. Write
 * operations to the elements of storage[] never set the element back to null; they either replace
 * it with a new element (put()) or the DELETED marker (remove()). 4. A probe sequence ends only
 * when an element with the desired key is found, or when null is found. In combination with (3),
 * this means that probe sequences for a given key can never get shorter, i.e., there is no way to
 * "cut off" the end of an existing probe sequence before a concurrent execution of get() has
 * reached its end. 5. This leaves the question of *when* the write of a new element to the array
 * might become visible to a get() operation in another thread. In theory, this can be an
 * arbitrarily long time; practically, it is not a problem, since current hardware will never delay
 * the propagation of a memory-write indefinitely.
 *
 * <p>The theoretical question in (5) can be resolved for good by adding a volatile int variable
 * which is written at the end of put() and read at the beginning of get(). See the implementation
 * of ConcurrentHashMap, and the memory-model specification JSR-133. In a nutshell, here is the
 * argument:
 *
 * <p>Given:
 *
 * <p>class foo { int a = 0; volatile int nonzero = false; void inThread1() { a++; nonzero = true; }
 * void inThread2() { if ( nonzero ) assert a != 0; } }
 *
 * <p>and assuming that inThread1() is called by a thread T1, and inThread2 is called concurrently
 * by a thread T2, then the method inThread2 will never throw an AssertionFailure.
 *
 * <p>T1:write(a) "happens-before" T1:write(foo) // program order, volatile reordering restriction
 * T1:write(foo) "happens-before" T2:read(foo) // volatile, introduces "synchronizes-with"
 * T2:read(foo) "happens-before" T2:read(a) // program order, volatile reordering restrictions
 * T1:write(a) "happens-before" t2:read(a) // "happens-before" is transitive
 *
 * <p>I haven't implemented it yet, though.
 */
public class KeyedDoubleObjectHash<V> extends KeyedObjectHash<Double, V> implements Serializable {

  /** The key definition for this instance. */
  private KeyedDoubleObjectKey<V> doubleKeyDef;

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with the default capacity and load factor.
   */
  public KeyedDoubleObjectHash(KeyedDoubleObjectKey<V> keyDef) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedDoubleObjectHash</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public KeyedDoubleObjectHash(int initialCapacity, KeyedDoubleObjectKey<V> keyDef) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedDoubleObjectHash</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public KeyedDoubleObjectHash(
      int initialCapacity, float loadFactor, KeyedDoubleObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, keyDef);
    this.doubleKeyDef = keyDef;
  }

  /** Swap the contents of this hash with another one. */
  protected void swap(KeyedObjectHash<Double, V> other) {
    super.swap(other);
    KeyedDoubleObjectKey<V> ktmp;
    if (other instanceof KeyedDoubleObjectHash) {
      ktmp = ((KeyedDoubleObjectHash) other).doubleKeyDef;
      ((KeyedDoubleObjectHash) other).doubleKeyDef = doubleKeyDef;
      doubleKeyDef = ktmp;
    }
  }

  /*
   ** Map/Set implementation -- accessors
   */

  /**
   * Return the mapping for the given id.
   *
   * @param key the key
   * @return the mapping, or null if no mapping is present
   */
  public V get(Object key) {
    double k = (Double) key;
    return get(k);
  }

  /**
   * Return the mapping for the given id.
   *
   * @param key the key
   * @return the mapping, or null if no mapping is present
   */
  public V get(double key) {
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = doubleKeyDef.hashDoubleKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length, firstIndex = index;
    V candidate = vs[index];

    if (candidate == null
        || (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate))) {
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = vs[index];
        if (candidate == null
            || (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate))) {
          return candidate;
        }
        if (index == firstIndex) {
          throw new IllegalStateException(
              "Cycle detected in probe sequence - concurrent get bug is still there");
        }
      }
    }
  }

  /**
   * Returns true if the map contains a mapping for the given key.
   *
   * @param key the key to search for
   * @return true, if the map contains a mapping for key
   */
  public boolean containsKey(Object key) {
    double k = (Double) key;
    return get(k) != null;
  }

  /**
   * Returns true if the map contains a mapping for the given key.
   *
   * @param key the key to search for
   * @return true, if the map contains a mapping for key
   */
  public boolean containsKey(double key) {
    return get(key) != null;
  }

  /*
   ** Map implementation -- mutators
   */

  public synchronized V put(Double key, V value) {
    if (!doubleKeyDef.equalDoubleKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(value));
    }
    return internalPut(value, KeyedDoubleObjectHash.NORMAL, null);
  }

  public synchronized V put(double key, V value) {
    if (!doubleKeyDef.equalDoubleKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(value));
    }
    return internalPut(value, KeyedDoubleObjectHash.NORMAL, null);
  }

  public synchronized V putIfAbsent(Double key, V value) {
    if (!doubleKeyDef.equalDoubleKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(value));
    }
    return internalPut(value, KeyedDoubleObjectHash.IF_ABSENT, null);
  }

  public synchronized V putIfAbsent(double key, V value) {
    if (!doubleKeyDef.equalDoubleKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(value));
    }
    return internalPut(value, KeyedDoubleObjectHash.IF_ABSENT, null);
  }

  public synchronized V replace(Double key, V value) {
    if (!doubleKeyDef.equalDoubleKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(value));
    }
    return internalPut(value, KeyedDoubleObjectHash.REPLACE, null);
  }

  public synchronized V replace(double key, V value) {
    if (!doubleKeyDef.equalDoubleKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(value));
    }
    return internalPut(value, KeyedDoubleObjectHash.REPLACE, null);
  }

  public synchronized boolean replace(Double key, V oldValue, V newValue) {
    if (oldValue == null) {
      throw new NullPointerException("oldValue is null, but this map cannot hold null values");
    }
    if (!doubleKeyDef.equalDoubleKey(key, newValue)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(newValue));
    }
    return Objects.equals(internalPut(newValue, KeyedDoubleObjectHash.REPLACE, oldValue), oldValue);
  }

  public synchronized boolean replace(double key, V oldValue, V newValue) {
    if (oldValue == null) {
      throw new NullPointerException("oldValue is null, but this map cannot hold null values");
    }
    if (!doubleKeyDef.equalDoubleKey(key, newValue)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + doubleKeyDef.getDoubleKey(newValue));
    }
    return Objects.equals(internalPut(newValue, KeyedDoubleObjectHash.REPLACE, oldValue), oldValue);
  }

  private static final int NORMAL = 0;
  private static final int IF_ABSENT = 1;
  private static final int REPLACE = 2;

  @Override
  protected V internalPut(V value, int mode, V oldValue) {
    V[] state = storage;
    double key = doubleKeyDef.getDoubleKey(value);
    final int length = state.length;
    final int hash = doubleKeyDef.hashDoubleKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      if (mode != KeyedDoubleObjectHash.REPLACE) {
        state[index] = value;
        _indexableList = null;
        postInsertHook(true);
      }
      return null; // reset the free element coun
    } else if (candidate == DELETED) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      // found a deleted slot -- remember its index, and continue until
      // we find the desired key or a virgin slot
      int firstDeleted = index;
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          if (mode != KeyedDoubleObjectHash.REPLACE) {
            state[firstDeleted] = value;
            _indexableList = null;
            postInsertHook(false);
          }
          return null;
        } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
          if (mode != KeyedDoubleObjectHash.IF_ABSENT
              && (oldValue == null || candidate.equals(oldValue))) {
            state[index] = value;
            _indexableList = null;
          }
          return candidate;
        }
      }
    } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
      if (mode != KeyedDoubleObjectHash.IF_ABSENT
          && (oldValue == null || candidate.equals(oldValue))) {
        state[index] = value;
        _indexableList = null;
      }
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          if (mode != KeyedDoubleObjectHash.REPLACE) {
            state[index] = value;
            _indexableList = null;
            postInsertHook(true);
          }
          return null;
        } else if (candidate == DELETED) {
          // found a deleted slot -- remember its index, and continue until
          // we find the desired key or a virgin slot
          int firstDeleted = index;
          while (true) {
            if ((index -= probe) < 0) {
              index += length;
            }
            candidate = state[index];
            if (candidate == null) {
              if (mode != KeyedDoubleObjectHash.REPLACE) {
                state[firstDeleted] = value;
                _indexableList = null;
                postInsertHook(false);
              }
              return null;
            } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
              if (mode != KeyedDoubleObjectHash.IF_ABSENT
                  && (oldValue == null || candidate.equals(oldValue))) {
                state[index] = value;
                _indexableList = null;
              }
              return candidate;
            }
          }
        } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
          if (mode != KeyedDoubleObjectHash.IF_ABSENT
              && (oldValue == null || candidate.equals(oldValue))) {
            state[index] = value;
            _indexableList = null;
          }
          return candidate;
        }
      }
    }
  }

  /**
   * If values can be constructed from just a key, this interface can be used with putIfAbsent(K,
   * ValueFactory) to implement an atomic find-or-add operation.
   */
  public interface ValueFactory<V> extends KeyedObjectHash.ValueFactory<Double, V> {
    V newValue(double key);

    public abstract static class Lax<V> implements ValueFactory<V> {
      public final V newValue(Double key) {
        return newValue((double) key);
      }
    }

    public abstract static class Strict<V> implements ValueFactory<V> {
      public final V newValue(Double key) {
        throw new IllegalArgumentException("Don't use boxed doubles please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and an int, this interface can be used with
   * putIfAbsent(K, ValueFactoryI, int) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryI<V> extends KeyedObjectHash.ValueFactoryI<Double, V> {
    V newValue(double key, int extra);

    public abstract static class Lax<V> implements ValueFactoryI<V> {
      public final V newValue(Double key, int extra) {
        return newValue((double) key, extra);
      }
    }

    public abstract static class Strict<V> implements ValueFactoryI<V> {
      public final V newValue(Double key, int extra) {
        throw new IllegalArgumentException("Don't use boxed doubles please");
      }
    }
  }

  /**
   * If values can be constructed from just a key, an int and a long, this interface can be used
   * with putIfAbsent(K, ValueFactoryIL, int, long) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryIL<V> extends KeyedObjectHash.ValueFactoryIL<Double, V> {
    V newValue(double key, int extra, long extra2);

    public abstract static class Lax<V> implements ValueFactoryIL<V> {
      public final V newValue(Double key, int extra, long extra2) {
        return newValue((double) key, extra, extra2);
      }
    }

    public abstract static class Strict<V> implements ValueFactoryIL<V> {
      public final V newValue(Double key, int extra, long extra2) {
        throw new IllegalArgumentException("Don't use boxed doubles please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and an object, this interface can be used with
   * putIfAbsent(K, ValueFactoryT, T) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryT<V, T> extends KeyedObjectHash.ValueFactoryT<Double, V, T> {
    V newValue(double key, T extra);

    public abstract static class Lax<V, T> implements ValueFactoryT<V, T> {
      public final V newValue(Double key, T extra) {
        return newValue((double) key, extra);
      }
    }

    public abstract static class Strict<V, T> implements ValueFactoryT<V, T> {
      public final V newValue(Double key, T extra) {
        throw new IllegalArgumentException("Don't use boxed doubles please");
      }
    }
  }

  public V putIfAbsent(double key, ValueFactory<V> factory) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsent(key, factory);
      }
    }
    return value;
  }

  public V putIfAbsent(double key, ValueFactoryI<V> factory, int extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentI(key, factory, extra);
      }
    }
    return value;
  }

  public V putIfAbsent(double key, ValueFactoryIL<V> factory, int extra, long extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentIL(key, factory, extra, extra2);
      }
    }
    return value;
  }

  public <T> V putIfAbsent(double key, ValueFactoryT<V, T> factory, T extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentT(key, factory, extra);
      }
    }
    return value;
  }

  private V internalCreateIfAbsent(double key, ValueFactory<V> factory) {
    V[] state = storage;
    final int length = state.length;
    final int hash = doubleKeyDef.hashDoubleKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      V new_v = factory.newValue(key);
      checkSameKey(key, new_v);
      state[index] = new_v;
      _indexableList = null;
      postInsertHook(true);
      return new_v;
    } else if (candidate == DELETED) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      // found a deleted slot -- remember its index, and continue until
      // we find the desired key or a virgin slot
      int firstDeleted = index;
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key);
          checkSameKey(key, new_v);
          state[firstDeleted] = new_v;
          _indexableList = null;
          postInsertHook(false);
          return new_v;
        } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key);
          checkSameKey(key, new_v);
          state[index] = new_v;
          _indexableList = null;
          postInsertHook(true);
          return new_v;
        } else if (candidate == DELETED) {
          // found a deleted slot -- remember its index, and continue until
          // we find the desired key or a virgin slot
          int firstDeleted = index;
          while (true) {
            if ((index -= probe) < 0) {
              index += length;
            }
            candidate = state[index];
            if (candidate == null) {
              V new_v = factory.newValue(key);
              checkSameKey(key, new_v);
              state[firstDeleted] = new_v;
              _indexableList = null;
              postInsertHook(false);
              return new_v;
            } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentI(double key, ValueFactoryI<V> factory, int extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = doubleKeyDef.hashDoubleKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      V new_v = factory.newValue(key, extra);
      checkSameKey(key, new_v);
      state[index] = new_v;
      _indexableList = null;
      postInsertHook(true);
      return new_v;
    } else if (candidate == DELETED) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      // found a deleted slot -- remember its index, and continue until
      // we find the desired key or a virgin slot
      int firstDeleted = index;
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key, extra);
          checkSameKey(key, new_v);
          state[firstDeleted] = new_v;
          _indexableList = null;
          postInsertHook(false);
          return new_v;
        } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key, extra);
          checkSameKey(key, new_v);
          state[index] = new_v;
          _indexableList = null;
          postInsertHook(true);
          return new_v;
        } else if (candidate == DELETED) {
          // found a deleted slot -- remember its index, and continue until
          // we find the desired key or a virgin slot
          int firstDeleted = index;
          while (true) {
            if ((index -= probe) < 0) {
              index += length;
            }
            candidate = state[index];
            if (candidate == null) {
              V new_v = factory.newValue(key, extra);
              checkSameKey(key, new_v);
              state[firstDeleted] = new_v;
              _indexableList = null;
              postInsertHook(false);
              return new_v;
            } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentIL(
      double key, ValueFactoryIL<V> factory, int extra, long extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = doubleKeyDef.hashDoubleKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      V new_v = factory.newValue(key, extra, extra2);
      checkSameKey(key, new_v);
      state[index] = new_v;
      _indexableList = null;
      postInsertHook(true);
      return new_v;
    } else if (candidate == DELETED) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      // found a deleted slot -- remember its index, and continue until
      // we find the desired key or a virgin slot
      int firstDeleted = index;
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key, extra, extra2);
          checkSameKey(key, new_v);
          state[firstDeleted] = new_v;
          _indexableList = null;
          postInsertHook(false);
          return new_v;
        } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key, extra, extra2);
          checkSameKey(key, new_v);
          state[index] = new_v;
          _indexableList = null;
          postInsertHook(true);
          return new_v;
        } else if (candidate == DELETED) {
          // found a deleted slot -- remember its index, and continue until
          // we find the desired key or a virgin slot
          int firstDeleted = index;
          while (true) {
            if ((index -= probe) < 0) {
              index += length;
            }
            candidate = state[index];
            if (candidate == null) {
              V new_v = factory.newValue(key, extra, extra2);
              checkSameKey(key, new_v);
              state[firstDeleted] = new_v;
              _indexableList = null;
              postInsertHook(false);
              return new_v;
            } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentT(double key, ValueFactoryT<V, T> factory, T extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = doubleKeyDef.hashDoubleKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      V new_v = factory.newValue(key, extra);
      checkSameKey(key, new_v);
      state[index] = new_v;
      _indexableList = null;
      postInsertHook(true);
      return new_v;
    } else if (candidate == DELETED) {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      // found a deleted slot -- remember its index, and continue until
      // we find the desired key or a virgin slot
      int firstDeleted = index;
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key, extra);
          checkSameKey(key, new_v);
          state[firstDeleted] = new_v;
          _indexableList = null;
          postInsertHook(false);
          return new_v;
        } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = state[index];
        if (candidate == null) {
          V new_v = factory.newValue(key, extra);
          checkSameKey(key, new_v);
          state[index] = new_v;
          _indexableList = null;
          postInsertHook(true);
          return new_v;
        } else if (candidate == DELETED) {
          // found a deleted slot -- remember its index, and continue until
          // we find the desired key or a virgin slot
          int firstDeleted = index;
          while (true) {
            if ((index -= probe) < 0) {
              index += length;
            }
            candidate = state[index];
            if (candidate == null) {
              V new_v = factory.newValue(key, extra);
              checkSameKey(key, new_v);
              state[firstDeleted] = new_v;
              _indexableList = null;
              postInsertHook(false);
              return new_v;
            } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (doubleKeyDef.equalDoubleKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  public synchronized V removeKey(Double key) {
    return internalRemove(key, null);
  }

  public synchronized V removeKey(double key) {
    return internalRemove(key, null);
  }

  public synchronized boolean remove(Double key, Object value) {
    return internalRemove(key, (V) value) != null;
  }

  public synchronized boolean remove(double key, Object value) {
    return internalRemove(key, (V) value) != null;
  }

  public synchronized boolean removeValue(V value) {
    return internalRemove(doubleKeyDef.getDoubleKey(value), null) != null;
  }

  protected V internalRemove(double key, V value) {
    V prev = null;
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = doubleKeyDef.hashDoubleKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = vs[index];

    if (candidate == null) {
      return null;
    } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
      prev = vs[index];
      if (value == null || candidate.equals(value)) {
        vs[index] = (V) DELETED;
        super.removeAt(index);
        _indexableList = null;
      }
      return prev;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = vs[index];
        if (candidate == null) {
          return null;
        } else if (candidate != DELETED && doubleKeyDef.equalDoubleKey(key, candidate)) {
          prev = vs[index];
          if (value == null || candidate.equals(value)) {
            vs[index] = (V) DELETED;
            super.removeAt(index);
            _indexableList = null;
          }
          return prev;
        }
      }
    }
  }

  private void checkSameKey(double key, V new_v) {
    if (!doubleKeyDef.equalDoubleKey(key, new_v)) {
      throw new RuntimeException(
          "NewValue key and key don't match. key def="
              + doubleKeyDef.getDoubleKey(new_v)
              + ", key="
              + key);
    }
  }
}

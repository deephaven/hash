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

/**
 * This collection implements a hashed set of objects identified by a key; the characteristics of
 * the key are defined by an instance of the KeyedObjectKey interface.
 *
 * <p>This class implements most of both the Set and ConcurrentMap interfaces. It can't fully
 * implement both because of discrepancies in the signatures and/or semantics of the remove(),
 * equals() and hashCode() methods. The subclasses KeyedObjectHashMap and KeyedObjectHashSet fill in
 * the missing bits.
 *
 * <p>In cases where the key to a map is just an attribute of the mapped value, this design will
 * save memory over an equivalent Map instance, since no extra slot is required to hold a reference
 * to (or in the worst case, a copy of) the key attribute. This can be significant if the key
 * attribute is a primitive int. We also use an open-addressed hash, so Map.Entry instance are also
 * unnecessary except for iteration over the entrySet() collection.
 *
 * <p>HOWEVER: note that open-addressed hashing may not be appropriate for tables whose populations
 * grow and shrink dynamically. This is because the markers for deleted slots can lengthen probe
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
public class KeyedIntObjectHash<V> extends KeyedObjectHash<Integer, V> implements Serializable {

  /** The key definition for this instance. */
  private KeyedIntObjectKey<V> intKeyDef;

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with the default capacity and load factor.
   */
  public KeyedIntObjectHash(KeyedIntObjectKey<V> keyDef) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedIntObjectHash</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public KeyedIntObjectHash(int initialCapacity, KeyedIntObjectKey<V> keyDef) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedIntObjectHash</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public KeyedIntObjectHash(int initialCapacity, float loadFactor, KeyedIntObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, keyDef);
    this.intKeyDef = keyDef;
  }

  /** Swap the contents of this hash with another one. */
  protected void swap(KeyedObjectHash<Integer, V> other) {
    super.swap(other);
    KeyedIntObjectKey<V> ktmp;
    if (other instanceof KeyedIntObjectHash) {
      ktmp = ((KeyedIntObjectHash) other).intKeyDef;
      ((KeyedIntObjectHash) other).intKeyDef = intKeyDef;
      intKeyDef = ktmp;
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
    int k = (Integer) key;
    return get(k);
  }

  /**
   * Return the mapping for the given id.
   *
   * @param key the key
   * @return the mapping, or null if no mapping is present
   */
  public V get(int key) {
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length, firstIndex = index;
    V candidate = vs[index];

    if (candidate == null || (candidate != DELETED && intKeyDef.equalIntKey(key, candidate))) {
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));
      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = vs[index];
        if (candidate == null || (candidate != DELETED && intKeyDef.equalIntKey(key, candidate))) {
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
    int k = (Integer) key;
    return get(k) != null;
  }

  /**
   * Returns true if the map contains a mapping for the given key.
   *
   * @param key the key to search for
   * @return true, if the map contains a mapping for key
   */
  public boolean containsKey(int key) {
    return get(key) != null;
  }

  /*
   ** Map implementation -- mutators
   */

  public synchronized V put(Integer key, V value) {
    if (!intKeyDef.equalIntKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(value));
    }
    return internalPut(value, KeyedIntObjectHash.NORMAL, null);
  }

  public synchronized V put(int key, V value) {
    if (!intKeyDef.equalIntKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(value));
    }
    return internalPut(value, KeyedIntObjectHash.NORMAL, null);
  }

  public synchronized V putIfAbsent(Integer key, V value) {
    if (!intKeyDef.equalIntKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(value));
    }
    return internalPut(value, KeyedIntObjectHash.IF_ABSENT, null);
  }

  public synchronized V putIfAbsent(int key, V value) {
    if (!intKeyDef.equalIntKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(value));
    }
    return internalPut(value, KeyedIntObjectHash.IF_ABSENT, null);
  }

  public synchronized V replace(Integer key, V value) {
    if (!intKeyDef.equalIntKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(value));
    }
    return internalPut(value, KeyedIntObjectHash.REPLACE, null);
  }

  public synchronized V replace(int key, V value) {
    if (!intKeyDef.equalIntKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(value));
    }
    return internalPut(value, KeyedIntObjectHash.REPLACE, null);
  }

  public synchronized boolean replace(Integer key, V oldValue, V newValue) {
    if (oldValue == null) {
      throw new NullPointerException("oldValue is null, but this map cannot hold null values");
    }
    if (!intKeyDef.equalIntKey(key, newValue)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(newValue));
    }
    return internalPut(newValue, KeyedIntObjectHash.REPLACE, oldValue).equals(oldValue);
  }

  public synchronized boolean replace(int key, V oldValue, V newValue) {
    if (oldValue == null) {
      throw new NullPointerException("oldValue is null, but this map cannot hold null values");
    }
    if (!intKeyDef.equalIntKey(key, newValue)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + intKeyDef.getIntKey(newValue));
    }
    return internalPut(newValue, KeyedIntObjectHash.REPLACE, oldValue).equals(oldValue);
  }

  private static final int NORMAL = 0;
  private static final int IF_ABSENT = 1;
  private static final int REPLACE = 2;

  @Override
  protected V internalPut(V value, int mode, V oldValue) {
    V[] state = storage;
    int key = intKeyDef.getIntKey(value);
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      if (mode != KeyedIntObjectHash.REPLACE) {
        state[index] = value;
        _indexableList = null;
        postInsertHook(true);
      }
      return null; // reset the free element count
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
          if (mode != KeyedIntObjectHash.REPLACE) {
            state[firstDeleted] = value;
            _indexableList = null;
            postInsertHook(false);
          }
          return null;
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          if (mode != KeyedIntObjectHash.IF_ABSENT
              && (oldValue == null || candidate.equals(oldValue))) {
            state[index] = value;
            _indexableList = null;
          }
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
      if (mode != KeyedIntObjectHash.IF_ABSENT
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
          if (mode != KeyedIntObjectHash.REPLACE) {
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
              if (mode != KeyedIntObjectHash.REPLACE) {
                state[firstDeleted] = value;
                _indexableList = null;
                postInsertHook(false);
              }
              return null;
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              if (mode != KeyedIntObjectHash.IF_ABSENT
                  && (oldValue == null || candidate.equals(oldValue))) {
                state[index] = value;
                _indexableList = null;
              }
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          if (mode != KeyedIntObjectHash.IF_ABSENT
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
  public interface ValueFactory<V> extends KeyedObjectHash.ValueFactory<Integer, V> {
    V newValue(int key);

    public abstract static class Lax<V> implements ValueFactory<V> {
      public final V newValue(Integer key) {
        return newValue((int) key);
      }
    }

    public abstract static class Strict<V> implements ValueFactory<V> {
      public final V newValue(Integer key) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and an int, this interface can be used with
   * putIfAbsent(K, ValueFactoryI, int) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryI<V> extends KeyedObjectHash.ValueFactoryI<Integer, V> {
    V newValue(int key, int extra);

    public abstract static class Lax<V> implements ValueFactoryI<V> {
      public final V newValue(Integer key, int extra) {
        return newValue((int) key, extra);
      }
    }

    public abstract static class Strict<V> implements ValueFactoryI<V> {
      public final V newValue(Integer key, int extra) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and a long, this interface can be used with
   * putIfAbsent(K, ValueFactoryL, long) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryL<V> extends KeyedObjectHash.ValueFactoryL<Integer, V> {
    V newValue(int key, long extra);

    public abstract static class Lax<V> implements ValueFactoryL<V> {
      public final V newValue(Integer key, long extra) {
        return newValue((int) key, extra);
      }
    }

    public abstract static class Strict<V> implements ValueFactoryL<V> {
      public final V newValue(Integer key, long extra) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and an int, this interface can be used with
   * putIfAbsent(K, ValueFactoryI, int) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryB<V> extends KeyedObjectHash.ValueFactoryB<Integer, V> {
    V newValue(int key, boolean extra);

    public abstract static class Lax<V> implements ValueFactoryB<V> {
      public final V newValue(Integer key, boolean extra) {
        return newValue((int) key, extra);
      }
    }

    public abstract static class Strict<V> implements ValueFactoryB<V> {
      public final V newValue(Integer key, boolean extra) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and an object, this interface can be used with
   * putIfAbsent(K, ValueFactoryT, T) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryT<V, T> extends KeyedObjectHash.ValueFactoryT<Integer, V, T> {
    V newValue(int key, T extra);

    public abstract static class Lax<V, T> implements ValueFactoryT<V, T> {
      public final V newValue(Integer key, T extra) {
        return newValue((int) key, extra);
      }
    }

    public abstract static class Strict<V, T> implements ValueFactoryT<V, T> {
      public final V newValue(Integer key, T extra) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  /**
   * If values can be constructed from just a key, an int and an object, this interface can be used
   * with putIfAbsent(K, ValueFactoryIT, int, T) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryIT<V, T> extends KeyedObjectHash.ValueFactoryIT<Integer, V, T> {
    V newValue(int key, int extra, T extra2);

    public abstract static class Lax<V, T> implements ValueFactoryIT<V, T> {
      public final V newValue(Integer key, int extra, T extra2) {
        return newValue((int) key, extra, extra2);
      }
    }

    public abstract static class Strict<V, T> implements ValueFactoryIT<V, T> {
      public final V newValue(Integer key, int extra, T extra2) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and two objects, this interface can be used with
   * putIfAbsent(K, ValueFactoryT, T1, T2) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryTT<V, T1, T2>
      extends KeyedObjectHash.ValueFactoryTT<Integer, V, T1, T2> {
    V newValue(int key, T1 extra1, T2 extra2);

    public abstract static class Lax<V, T1, T2> implements ValueFactoryTT<V, T1, T2> {
      public final V newValue(Integer key, T1 extra1, T2 extra2) {
        return newValue((int) key, extra1, extra2);
      }
    }

    public abstract static class Strict<V, T1, T2> implements ValueFactoryTT<V, T1, T2> {
      public final V newValue(Integer key, T1 extra1, T2 extra2) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and three objects, this interface can be used with
   * putIfAbsent(K, ValueFactoryT, T1, T2, T3) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryTTT<V, T1, T2, T3>
      extends KeyedObjectHash.ValueFactoryTTT<Integer, V, T1, T2, T3> {
    V newValue(int key, T1 extra1, T2 extra2, T3 extra3);

    public abstract static class Lax<V, T1, T2, T3> implements ValueFactoryTTT<V, T1, T2, T3> {
      public final V newValue(Integer key, T1 extra1, T2 extra2, T3 extra3) {
        return newValue((int) key, extra1, extra2, extra3);
      }
    }

    public abstract static class Strict<V, T1, T2, T3> implements ValueFactoryTTT<V, T1, T2, T3> {
      public final V newValue(Integer key, T1 extra1, T2 extra2, T3 extra3) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  public V putIfAbsent(int key, ValueFactory<V> factory) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsent(key, factory);
      }
    }
    return value;
  }

  public V putIfAbsent(int key, ValueFactoryI<V> factory, int extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentI(key, factory, extra);
      }
    }
    return value;
  }

  public V putIfAbsent(int key, ValueFactoryL<V> factory, long extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentL(key, factory, extra);
      }
    }
    return value;
  }

  public V putIfAbsent(int key, ValueFactoryB<V> factory, boolean extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentB(key, factory, extra);
      }
    }
    return value;
  }

  public <T> V putIfAbsent(int key, ValueFactoryT<V, T> factory, T extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentT(key, factory, extra);
      }
    }
    return value;
  }

  public <T> V putIfAbsent(int key, ValueFactoryIT<V, T> factory, int extra, T extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentIT(key, factory, extra, extra2);
      }
    }
    return value;
  }

  public <T1, T2> V putIfAbsent(int key, ValueFactoryTT<V, T1, T2> factory, T1 extra1, T2 extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentTT(key, factory, extra1, extra2);
      }
    }
    return value;
  }

  public <T1, T2, T3> V putIfAbsent(
      int key, ValueFactoryTTT<V, T1, T2, T3> factory, T1 extra1, T2 extra2, T3 extra3) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentTTT(key, factory, extra1, extra2, extra3);
      }
    }
    return value;
  }

  private V internalCreateIfAbsent(int key, ValueFactory<V> factory) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentI(int key, ValueFactoryI<V> factory, int extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentL(int key, ValueFactoryL<V> factory, long extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentB(int key, ValueFactoryB<V> factory, boolean extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentT(int key, ValueFactoryT<V, T> factory, T extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentIT(
      int key, ValueFactoryIT<V, T> factory, int extra, T extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T1, T2> V internalCreateIfAbsentTT(
      int key, ValueFactoryTT<V, T1, T2> factory, T1 extra1, T2 extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      V new_v = factory.newValue(key, extra1, extra2);
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
          V new_v = factory.newValue(key, extra1, extra2);
          checkSameKey(key, new_v);
          state[firstDeleted] = new_v;
          _indexableList = null;
          postInsertHook(false);
          return new_v;
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
          V new_v = factory.newValue(key, extra1, extra2);
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
              V new_v = factory.newValue(key, extra1, extra2);
              checkSameKey(key, new_v);
              state[firstDeleted] = new_v;
              _indexableList = null;
              postInsertHook(false);
              return new_v;
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T1, T2, T3> V internalCreateIfAbsentTTT(
      int key, ValueFactoryTTT<V, T1, T2, T3> factory, T1 extra1, T2 extra2, T3 extra3) {
    V[] state = storage;
    final int length = state.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      V new_v = factory.newValue(key, extra1, extra2, extra3);
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
          V new_v = factory.newValue(key, extra1, extra2, extra3);
          checkSameKey(key, new_v);
          state[firstDeleted] = new_v;
          _indexableList = null;
          postInsertHook(false);
          return new_v;
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (intKeyDef.equalIntKey(key, candidate)) {
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
          V new_v = factory.newValue(key, extra1, extra2, extra3);
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
              V new_v = factory.newValue(key, extra1, extra2, extra3);
              checkSameKey(key, new_v);
              state[firstDeleted] = new_v;
              _indexableList = null;
              postInsertHook(false);
              return new_v;
            } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (intKeyDef.equalIntKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  public synchronized V removeKey(Integer key) {
    return internalRemove(key, null);
  }

  public synchronized V removeKey(int key) {
    return internalRemove(key, null);
  }

  public synchronized boolean remove(Integer key, Object value) {
    return internalRemove(key, (V) value) != null;
  }

  public synchronized boolean remove(int key, Object value) {
    return internalRemove(key, (V) value) != null;
  }

  public synchronized boolean removeValue(V value) {
    return internalRemove(intKeyDef.getIntKey(value), null) != null;
  }

  protected V internalRemove(int key, V value) {
    V prev = null;
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = intKeyDef.hashIntKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = vs[index];

    if (candidate == null) {
      return null;
    } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
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
        } else if (candidate != DELETED && intKeyDef.equalIntKey(key, candidate)) {
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

  private void checkSameKey(int key, V new_v) {
    if (!intKeyDef.equalIntKey(key, new_v)) {
      throw new RuntimeException(
          "NewValue key and key don't match. key def="
              + intKeyDef.getIntKey(new_v)
              + ", key="
              + key);
    }
  }
}

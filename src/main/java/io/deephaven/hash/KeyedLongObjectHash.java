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
public class KeyedLongObjectHash<V> extends KeyedObjectHash<Long, V> implements Serializable {

  /** The key definition for this instance. */
  private KeyedLongObjectKey<V> longKeyDef;

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with the default capacity and load factor.
   */
  public KeyedLongObjectHash(KeyedLongObjectKey<V> keyDef) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedLongObjectHash</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public KeyedLongObjectHash(int initialCapacity, KeyedLongObjectKey<V> keyDef) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedIntObjectHash</code> instance with a prime capacity equal to or
   * greater than <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public KeyedLongObjectHash(int initialCapacity, float loadFactor, KeyedLongObjectKey<V> keyDef) {
    super(initialCapacity, loadFactor, keyDef);
    this.longKeyDef = keyDef;
  }

  /** Swap the contents of this hash with another one. */
  protected void swap(KeyedObjectHash<Long, V> other) {
    super.swap(other);
    KeyedLongObjectKey<V> ktmp;
    if (other instanceof KeyedLongObjectHash) {
      ktmp = ((KeyedLongObjectHash) other).longKeyDef;
      ((KeyedLongObjectHash) other).longKeyDef = longKeyDef;
      longKeyDef = ktmp;
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
    long k = (Long) key;
    return get(k);
  }

  /**
   * Return the mapping for the given id.
   *
   * @param key the key
   * @return the mapping, or null if no mapping is present
   */
  public V get(long key) {
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length, firstIndex = index;
    V candidate = vs[index];

    if (candidate == null || (candidate != DELETED && longKeyDef.equalLongKey(key, candidate))) {
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
            || (candidate != DELETED && longKeyDef.equalLongKey(key, candidate))) {
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
    long k = (Long) key;
    return get(k) != null;
  }

  /**
   * Returns true if the map contains a mapping for the given key.
   *
   * @param key the key to search for
   * @return true, if the map contains a mapping for key
   */
  public boolean containsKey(long key) {
    return get(key) != null;
  }

  /*
   ** Map implementation -- mutators
   */

  public synchronized V put(Long key, V value) {
    if (!longKeyDef.equalLongKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(value));
    }
    return internalPut(value, KeyedLongObjectHash.NORMAL, null);
  }

  public synchronized V put(long key, V value) {
    if (!longKeyDef.equalLongKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(value));
    }
    return internalPut(value, KeyedLongObjectHash.NORMAL, null);
  }

  public synchronized V putIfAbsent(Long key, V value) {
    if (!longKeyDef.equalLongKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(value));
    }
    return internalPut(value, KeyedLongObjectHash.IF_ABSENT, null);
  }

  public synchronized V putIfAbsent(long key, V value) {
    if (!longKeyDef.equalLongKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(value));
    }
    return internalPut(value, KeyedLongObjectHash.IF_ABSENT, null);
  }

  public synchronized V replace(Long key, V value) {
    if (!longKeyDef.equalLongKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(value));
    }
    return internalPut(value, KeyedLongObjectHash.REPLACE, null);
  }

  public synchronized V replace(long key, V value) {
    if (!longKeyDef.equalLongKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(value));
    }
    return internalPut(value, KeyedLongObjectHash.REPLACE, null);
  }

  public synchronized boolean replace(Long key, V oldValue, V newValue) {
    if (!longKeyDef.equalLongKey(key, newValue)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(newValue));
    }
    return Objects.equals(internalPut(newValue, KeyedLongObjectHash.REPLACE, oldValue), oldValue);
  }

  public synchronized boolean replace(long key, V oldValue, V newValue) {
    if (!longKeyDef.equalLongKey(key, newValue)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + longKeyDef.getLongKey(newValue));
    }
    return Objects.equals(internalPut(newValue, KeyedLongObjectHash.REPLACE, oldValue), oldValue);
  }

  private static final int NORMAL = 0;
  private static final int IF_ABSENT = 1;
  private static final int REPLACE = 2;

  @Override
  protected V internalPut(V value, int mode, V oldValue) {
    V[] state = storage;
    long key = longKeyDef.getLongKey(value);
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      if (mode != KeyedLongObjectHash.REPLACE) {
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
          if (mode != KeyedLongObjectHash.REPLACE) {
            state[firstDeleted] = value;
            _indexableList = null;
            postInsertHook(false);
          }
          return null;
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          if (mode != KeyedLongObjectHash.IF_ABSENT
              && (oldValue == null || candidate.equals(oldValue))) {
            state[index] = value;
            _indexableList = null;
          }
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
      if (mode != KeyedLongObjectHash.IF_ABSENT
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
          if (mode != KeyedLongObjectHash.REPLACE) {
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
              if (mode != KeyedLongObjectHash.REPLACE) {
                state[firstDeleted] = value;
                _indexableList = null;
                postInsertHook(false);
              }
              return null;
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              if (mode != KeyedLongObjectHash.IF_ABSENT
                  && (oldValue == null || candidate.equals(oldValue))) {
                state[index] = value;
                _indexableList = null;
              }
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          if (mode != KeyedLongObjectHash.IF_ABSENT
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
  public interface ValueFactory<V> extends KeyedObjectHash.ValueFactory<Long, V> {
    V newValue(long key);

    public abstract static class Lax<V> implements ValueFactory<V> {
      public final V newValue(Long key) {
        return newValue((long) key);
      }
    }

    public abstract static class Strict<V> implements ValueFactory<V> {
      public final V newValue(Long key) {
        throw new IllegalArgumentException("Don't use boxed longs please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and an int, this interface can be used with
   * putIfAbsent(K, ValueFactoryI, int) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryI<V> extends KeyedObjectHash.ValueFactoryI<Long, V> {
    V newValue(long key, int extra);

    public abstract static class Lax<V> implements ValueFactoryI<V> {
      public final V newValue(Long key, int extra) {
        return newValue((long) key, extra);
      }
    }

    public abstract static class Strict<V> implements ValueFactoryI<V> {
      public final V newValue(Long key, int extra) {
        throw new IllegalArgumentException("Don't use boxed longs please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and a long, this interface can be used with
   * putIfAbsent(K, ValueFactoryL, long) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryL<V> extends KeyedObjectHash.ValueFactoryL<Long, V> {
    V newValue(long key, long extra);

    public abstract static class Lax<V> implements ValueFactoryL<V> {
      public final V newValue(Long key, long extra) {
        return newValue((long) key, extra);
      }
    }

    public abstract static class Strict<V> implements ValueFactoryL<V> {
      public final V newValue(Long key, long extra) {
        throw new IllegalArgumentException("Don't use boxed longs please");
      }
    }
  }

  /**
   * If values can be constructed from just a key and an object, this interface can be used with
   * putIfAbsent(K, ValueFactoryT, T) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryT<V, T> extends KeyedObjectHash.ValueFactoryT<Long, V, T> {
    V newValue(long key, T extra);

    public abstract static class Lax<V, T> implements ValueFactoryT<V, T> {
      public final V newValue(Long key, T extra) {
        return newValue((long) key, extra);
      }
    }

    public abstract static class Strict<V, T> implements ValueFactoryT<V, T> {
      public final V newValue(Long key, T extra) {
        throw new IllegalArgumentException("Don't use boxed longs please");
      }
    }
  }

  public interface ValueFactoryTT<V, T, T2> extends KeyedObjectHash.ValueFactoryTT<Long, V, T, T2> {
    V newValue(long key, T extra, T2 extra2);

    public abstract static class Lax<V, T, T2> implements ValueFactoryTT<V, T, T2> {
      public final V newValue(Long key, T extra, T2 extra2) {
        return newValue((long) key, extra, extra2);
      }
    }

    public abstract static class Strict<V, T, T2> implements ValueFactoryTT<V, T, T2> {
      public final V newValue(Long key, T extra, T2 extra2) {
        throw new IllegalArgumentException("Don't use boxed longs please");
      }
    }
  }

  public interface ValueFactoryIT<V, T> extends KeyedObjectHash.ValueFactoryIT<Long, V, T> {
    V newValue(long key, int extra, T extra2);

    public abstract static class Lax<V, T> implements ValueFactoryIT<V, T> {
      public final V newValue(Long key, int extra, T extra2) {
        return newValue((long) key, extra, extra2);
      }
    }

    public abstract static class Strict<V, T> implements ValueFactoryIT<V, T> {
      public final V newValue(Long key, int extra, T extra2) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  public interface ValueFactoryIIT<V, T> extends KeyedObjectHash.ValueFactoryIIT<Long, V, T> {
    V newValue(long key, int extra, int extra2, T extra3);

    public abstract static class Lax<V, T> implements ValueFactoryIIT<V, T> {
      public final V newValue(Long key, int extra, int extra2, T extra3) {
        return newValue((long) key, extra, extra2, extra3);
      }
    }

    public abstract static class Strict<V, T> implements ValueFactoryIIT<V, T> {
      public final V newValue(Long key, int extra, int extra2, T extra3) {
        throw new IllegalArgumentException("Don't use boxed integers please");
      }
    }
  }

  public V putIfAbsent(long key, ValueFactory<V> factory) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsent(key, factory);
      }
    }
    return value;
  }

  public V putIfAbsent(long key, ValueFactoryI<V> factory, int extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentI(key, factory, extra);
      }
    }
    return value;
  }

  public V putIfAbsent(long key, ValueFactoryL<V> factory, long extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentL(key, factory, extra);
      }
    }
    return value;
  }

  public <T> V putIfAbsent(long key, ValueFactoryT<V, T> factory, T extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentT(key, factory, extra);
      }
    }
    return value;
  }

  public <T, T2> V putIfAbsent(long key, ValueFactoryTT<V, T, T2> factory, T extra, T2 extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentTT(key, factory, extra, extra2);
      }
    }
    return value;
  }

  public <T> V putIfAbsent(long key, ValueFactoryIT<V, T> factory, int extra, T extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentIT(key, factory, extra, extra2);
      }
    }
    return value;
  }

  public <T> V putIfAbsent(
      long key, ValueFactoryIIT<V, T> factory, int extra, int extra2, T extra3) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentIIT(key, factory, extra, extra2, extra3);
      }
    }
    return value;
  }

  private V internalCreateIfAbsent(long key, ValueFactory<V> factory) {
    V[] state = storage;
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
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
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentI(long key, ValueFactoryI<V> factory, int extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
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
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentL(long key, ValueFactoryL<V> factory, long extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
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
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentT(long key, ValueFactoryT<V, T> factory, T extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
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
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentIIT(
      long key, ValueFactoryIIT<V, T> factory, int extra, int extra2, T extra3) {
    V[] state = storage;
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      V new_v = factory.newValue(key, extra, extra2, extra3);
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
          V new_v = factory.newValue(key, extra, extra2, extra3);
          checkSameKey(key, new_v);
          state[firstDeleted] = new_v;
          _indexableList = null;
          postInsertHook(false);
          return new_v;
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
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
          V new_v = factory.newValue(key, extra, extra2, extra3);
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
              V new_v = factory.newValue(key, extra, extra2, extra3);
              checkSameKey(key, new_v);
              state[firstDeleted] = new_v;
              _indexableList = null;
              postInsertHook(false);
              return new_v;
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentIT(
      long key, ValueFactoryIT<V, T> factory, int extra, T extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
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
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T, T2> V internalCreateIfAbsentTT(
      long key, ValueFactoryTT<V, T, T2> factory, T extra, T2 extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (longKeyDef.equalLongKey(key, candidate)) {
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
            } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (longKeyDef.equalLongKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  public synchronized V removeKey(Long key) {
    return internalRemove(key, null);
  }

  public synchronized V removeKey(long key) {
    return internalRemove(key, null);
  }

  public synchronized boolean remove(Long key, Object value) {
    return internalRemove(key, (V) value) != null;
  }

  public synchronized boolean remove(long key, Object value) {
    return internalRemove(key, (V) value) != null;
  }

  public synchronized boolean removeValue(V value) {
    return internalRemove(longKeyDef.getLongKey(value), null) != null;
  }

  protected V internalRemove(long key, V value) {
    V prev = null;
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = longKeyDef.hashLongKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = vs[index];

    if (candidate == null) {
      return null;
    } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
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
        } else if (candidate != DELETED && longKeyDef.equalLongKey(key, candidate)) {
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

  private void checkSameKey(long key, V new_v) {
    if (!longKeyDef.equalLongKey(key, new_v)) {
      throw new RuntimeException(
          "NewValue key and key don't match. key def="
              + longKeyDef.getLongKey(new_v)
              + ", key="
              + key);
    }
  }
}

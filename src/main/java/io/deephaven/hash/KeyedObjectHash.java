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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * This collection implements a hashed set of objects identified by a key; the characteristics of
 * the key are defined by an instance of the KeyedObjectKey interface.
 *
 * <p>This class implements most of both the Set<V> and ConcurrentMap<K,V> interfaces. It can't
 * fully implement both because of discrepancies in the signatures and/or semantics of the remove(),
 * equals() and hashCode() methods. The subclasses KeyedObjectHashMap and KeyedObjectHashSet fill in
 * the missing bits.
 *
 * <p>In cases where the key to a map is just an attribute of the mapped value, this design wil save
 * memory over an equivalent Map<K,V> instance, since no extra slot is required to hold a reference
 * to (or in the worst case, a copy of) the key attribute. This can be significant if the key
 * attribute is a primitive int. We also use an open-addressed hash, so Map.Entry instance are also
 * unnecessary except for iteration over the entrySet() collection.
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
 * put() call and a single get() call.
 *
 * <p>2. The only field which get() reads is storage[], which is initialized and in a consistent
 * state as soon as the object is constructed. It copies the reference to storage[] into a local
 * variable, so it cannot be affected by a rehash.
 *
 * <p>3. Write operations to the elements of storage[] never set the element back to null; they
 * either replace it with a new element (put()) or the DELETED marker (remove()).
 *
 * <p>4. A probe sequence ends only when an element with the desired key is found, or when null is
 * found. In combination with (3), this means that probe sequences for a given key can never get
 * shorter, i.e., there is no way to "cut off" the end of an existing probe sequence before a
 * concurrent execution of get() has reached its end.
 *
 * <p>5. This leaves the question of *when* the write of a new element to the array might become
 * visible to a get() operation in another thread. In theory, this can be an arbitrarily long time;
 * practically, it is not a problem, since current hardware will never delay the propagation of a
 * memory-write indefinitely.
 *
 * <p>The theoretical question in (5) can be resolved for good by adding a volatile int variable
 * which is written at the end of put() and read at the beginning of get(). See the implementation
 * of ConcurrentHashMap, and the memory-model specification JSR-133. In a nutshell, here is the
 * argument:
 *
 * <p>Given:
 *
 * <Pre>
 *    class foo {
 *      int a = 0;
 *      volatile int nonzero = false;
 *      void inThread1() { a++;  nonzero = true; }
 *      void inThread2() { if ( nonzero ) assert a != 0; }
 *    }
 * </Pre>
 *
 * and assuming that inThread1() is called by a thread T1, and inThread2 is called concurrently by a
 * thread T2, then the method inThread2 will never throw an AssertionFailure.
 *
 * <Pre>
 *    T1:write(a) "happens-before" T1:write(foo)     // program order, volatile reordering restriction
 *    T1:write(foo) "happens-before" T2:read(foo)    // volatile, introduces "synchronizes-with"
 *    T2:read(foo) "happens-before" T2:read(a)       // program order, volatile reordering restrictions
 *    T1:write(a) "happens-before" t2:read(a)        // "happens-before" is transitive
 * </Pre>
 *
 * I haven't implemented it yet, though.
 */
public class KeyedObjectHash<K, V> extends KHash implements Serializable, Iterable<V> {

  /** The key definition for this instance. */
  private KeyedObjectKey<K, V> keyDef;

  /** The storage for this instance. */
  protected V storage[];

  /** The marker for deleted slots. */
  protected static final Object DELETED = new Object();

  /** An array list view for IndexableMap implementation */
  protected transient ArrayList<V> _indexableList = null;

  /** compatible serialization ID - not present in 1.1b3 and earlier */
  static final long serialVersionUID = 1L;

  /** @return true if this is a valid slot */
  protected final boolean isValid(V v) {
    return v != null && v != DELETED;
  }

  /** @return the capacity of the underlying storage */
  public int capacity() {
    return storage.length;
  }

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with the default capacity and load factor.
   */
  public KeyedObjectHash(KeyedObjectKey<K, V> keyDef) {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with a prime capacity equal to or greater
   * than <tt>initialCapacity</tt> and with the default load factor.
   *
   * @param initialCapacity an <code>int</code> value
   */
  public KeyedObjectHash(int initialCapacity, KeyedObjectKey<K, V> keyDef) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR, keyDef);
  }

  /**
   * Creates a new <code>KeyedObjectHash</code> instance with a prime capacity equal to or greater
   * than <tt>initialCapacity</tt> and with the specified load factor.
   *
   * @param initialCapacity an <code>int</code> value
   * @param loadFactor a <code>float</code> value
   */
  public KeyedObjectHash(int initialCapacity, float loadFactor, KeyedObjectKey<K, V> keyDef) {
    super(initialCapacity, loadFactor);
    this.keyDef = keyDef;
  }

  /**
   * initializes the hashtable to a prime capacity which is at least <tt>initialCapacity + 1</tt>.
   * Called from the THash constructors.
   *
   * @param initialCapacity an <code>int</code> value
   * @return the actual capacity chosen
   */
  protected int setUp(int initialCapacity) {
    int capacity = super.setUp(initialCapacity);
    storage = (V[]) new Object[capacity];
    return capacity;
  }

  /**
   * rehashes the map to the new capacity.
   *
   * @param newCapacity an <code>int</code> value
   */
  protected void rehash(int newCapacity) {
    KeyedObjectHash<K, V> h = (KeyedObjectHash) super.clone();
    // HACK: this make entirely too much use of the base class implementation
    h._size = 0;
    h._maxSize = Math.min(newCapacity - 1, (int) Math.floor(newCapacity * h._loadFactor));
    h._free = newCapacity - h._size;
    h.storage = (V[]) new Object[newCapacity];
    for (V v : storage) {
      if (v != null && v != DELETED) {
        h.internalPut(v, NORMAL, null);
      }
    }
    swap(h);
  }

  /**
   * Return a copy of this hash.
   *
   * @return the cloned hash
   */
  public Object clone() {
    KeyedObjectHash h = (KeyedObjectHash) super.clone();
    h.storage = this.storage.clone();
    return h;
  }

  /** Make the hash table as small as possible. */
  public synchronized void compact() {
    super.compact();
  }

  /** Swap the contents of this hash with another one. */
  protected void swap(KeyedObjectHash<K, V> other) {
    int itmp;
    float ftmp;
    KeyedObjectKey<K, V> ktmp;
    V[] stmp;
    ArrayList<V> ltmp;

    // DANGER: these members are from the gnu.trove.THash base class; if that
    // implementation changes, these will also have to change.
    itmp = other._size;
    other._size = _size;
    _size = itmp;
    itmp = other._free;
    other._free = _free;
    _free = itmp;
    itmp = other._maxSize;
    other._maxSize = _maxSize;
    _maxSize = itmp;
    ftmp = other._loadFactor;
    other._loadFactor = _loadFactor;
    _loadFactor = ftmp;

    // these are the member we control directly
    ktmp = other.keyDef;
    other.keyDef = keyDef;
    keyDef = ktmp;
    stmp = other.storage;
    other.storage = storage;
    storage = stmp;
    ltmp = other._indexableList;
    other._indexableList = _indexableList;
    _indexableList = ltmp;
  }

  /*
   ** Serialization implementation
   */

  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.defaultWriteObject();
    stream.writeInt(_size);
    V[] es = storage;
    for (int i = es.length; i-- > 0; ) {
      V candidate = es[i];
      if (candidate != null && candidate != DELETED) {
        stream.writeObject(candidate);
      }
    }
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    int size = stream.readInt();
    setUp(size);
    while (size-- > 0) {
      internalPut((V) stream.readObject(), NORMAL, null);
    }
  }

  /** An Entry class for IntObject maps. */
  private class Entry implements Map.Entry<K, V> {
    private V value;

    public Entry(V v) {
      value = v;
    }

    public K getKey() {
      return keyDef.getKey(value);
    }

    public V getValue() {
      return value;
    }

    public boolean equals(final Object other) {
      return other instanceof Map.Entry
          && ((Map.Entry) other).getKey().equals(keyDef.getKey(value))
          && ((Map.Entry) other).getValue().equals(value);
    }

    public int hashCode() {
      // NOTE: this conforms to the specification of java.util.Map.Entry.hashCode()
      return keyDef.getKey(value).hashCode() ^ value.hashCode();
    }

    public V setValue(V v) {
      // sorry, can't do this one...
      throw new UnsupportedOperationException();
    }
  }

  /*
   ** A KeySet view for IntObject maps.
   */
  private class KeySet extends AbstractSet<K> {
    public Iterator<K> iterator() {
      return new KeyIterator();
    }

    public boolean contains(Object o) {
      return KeyedObjectHash.this.containsKey(o);
    }

    public boolean remove(Object o) {
      return KeyedObjectHash.this.removeKey((K) o) != null;
    }

    public int size() {
      return KeyedObjectHash.this.size();
    }

    public void clear() {
      KeyedObjectHash.this.clear();
    }
  }

  /*
   ** A Value collection view for IntObject maps.
   */
  private class ValueCollection extends AbstractCollection<V> {
    public Iterator<V> iterator() {
      return new ValueIterator();
    }

    public boolean contains(Object o) {
      return KeyedObjectHash.this.containsValue(o);
    }

    public boolean remove(Object o) {
      return KeyedObjectHash.this.removeValue((V) o);
    }

    public int size() {
      return KeyedObjectHash.this.size();
    }

    public void clear() {
      KeyedObjectHash.this.clear();
    }
  }

  /*
   ** An EntrySet view for KeyedObjectHashes.
   */
  private class EntrySet extends AbstractSet<Map.Entry<K, V>> {
    public Iterator<Map.Entry<K, V>> iterator() {
      return new EntryIterator();
    }

    public boolean add(Map.Entry<K, V> entry) {
      return KeyedObjectHash.this.put(entry.getKey(), entry.getValue()) == null;
    }

    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry entry = (Map.Entry) o;
      K key = (K) entry.getKey();
      V value = get(key);
      Object otherValue = entry.getValue();
      return (value == otherValue) || (value != null && value.equals(entry.getValue()));
    }

    public boolean remove(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry entry = (Map.Entry) o;
      return KeyedObjectHash.this.removeKey((K) entry.getKey()) != null;
    }

    public int size() {
      return KeyedObjectHash.this.size();
    }

    public void clear() {
      KeyedObjectHash.this.clear();
    }
  }

  /*
   ** An iterator implementation for IntObject maps.
   */
  private class BaseIterator {
    /** The snapshotted map storage. */
    V[] vs;

    /**
     * The current position in the iteration. Each call to next() returns the first filled slot
     * whose position is >= pos.
     */
    int pos;

    /** This is the last element returned. It is the one that remove() will remove. */
    V lastValue;

    /** The constructor just initializes the position to zero. */
    BaseIterator() {
      this.vs = KeyedObjectHash.this.storage;
      this.pos = 0;
      this.lastValue = null;
    }

    /** We have a next element if there is a filled slot at any position >= p. */
    public boolean hasNext() {
      for (int p = pos; p < vs.length; ++p) {
        if (isValid(vs[p])) {
          return true;
        }
      }
      return false;
    }

    protected V scan() {
      while (pos < vs.length) {
        if (isValid(vs[pos])) {
          return lastValue = vs[pos++];
        }
        ++pos;
      }
      throw new NoSuchElementException("KeyedObjectHash Iterator");
    }

    public void remove() {
      KeyedObjectHash.this.removeValue(lastValue);
    }
  }

  private class EntryIterator extends BaseIterator implements Iterator<Map.Entry<K, V>> {
    public Map.Entry<K, V> next() {
      return new KeyedObjectHash.Entry(super.scan());
    }
  }

  private class KeyIterator extends BaseIterator implements Iterator<K> {
    public K next() {
      return keyDef.getKey(super.scan());
    }
  }

  private class ValueIterator extends BaseIterator implements Iterator<V> {
    public V next() {
      return (V) super.scan();
    }
  }

  /*
   ** Map/Set implementation -- views and iterators
   */

  public Set<K> keySet() {
    return new KeySet();
  }

  public Collection<V> values() {
    return new ValueCollection();
  }

  public Set<Map.Entry<K, V>> entrySet() {
    return new EntrySet();
  }

  public Iterator<V> iterator() {
    return new ValueIterator();
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
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = keyDef.hashKey((K) key) & 0x7FFFFFFF;

    int probe, index = hash % length, firstIndex = index;
    V candidate = vs[index];

    if (candidate == null || (candidate != DELETED && keyDef.equalKey((K) key, candidate))) {
      return candidate;
    } else {
      // see Knuth, p. 529
      probe = 1 + (hash % (length - 2));

      while (true) {
        if ((index -= probe) < 0) {
          index += length;
        }
        candidate = vs[index];
        if (candidate == null || (candidate != DELETED && keyDef.equalKey((K) key, candidate))) {
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
    return get(key) != null;
  }

  /**
   * Returns true if the map contains a mapping whose value is equals() to value. Warning: this is
   * O(N) in the underlying size of the map.
   *
   * @param value
   * @return true, if the map contains the given value
   */
  public boolean containsValue(Object value) {
    V[] vs = storage;
    for (V v : vs) {
      if (v == value || (v != null && v.equals(value))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the set contains the given object.
   *
   * @param value
   * @return true, if the set contains the given object
   */
  public boolean contains(Object value) {
    return containsValue(value);
  }

  /**
   * Returns true if the set contains all of the elements in c
   *
   * @param c the collection of objects
   * @return true, if the set contains all of the elements in c
   */
  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns a newly allocated array containing all of the elements in the set.
   *
   * @return the array
   */
  public Object[] toArray() {
    int i = 0;
    Object[] result = new Object[size()];
    for (V v : storage) {
      if (v != null && v != DELETED) {
        if (i < result.length) {
          result[i++] = v;
        } else {
          return result;
        }
      }
    }
    if (i < result.length) {
      // oh damn, something got removed while we were scanning - we can't
      // return an array with null elements at the end.
      Object[] newResult = new Object[i];
      System.arraycopy(result, 0, newResult, 0, i);
      result = newResult;
    }
    return result;
  }

  /**
   * Returns all of the elements in the set in the array a; if a is not large enough, a new array
   * with the same runtime type is allocated and returned. See the specification of
   * java.util.Collection.toArray.
   *
   * @param a the desination array
   * @return either a, or a new array with the same runtime type.
   */
  public <T> T[] toArray(T[] a) {
    if (size() > a.length) {
      a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size());
    }
    int i = 0;
    Object[] result = a;
    for (V v : storage) {
      if (v != null && v != DELETED) {
        if (i < result.length) {
          result[i++] = v;
        } else {
          return (T[]) result;
        }
      }
    }
    if (i < result.length) {
      result[i] = null;
    }
    return (T[]) result;
  }

  /**
   * Returns all of the elements in the set that match a predicate in the array a; if a is not large
   * enough, a new array with the same runtime type is allocated and returned. See the specification
   * of java.util.Collection.toArray.
   *
   * @param a the desination array
   * @return either a, or a new array with the same runtime type.
   */
  public <T> T[] toArray(T[] a, Predicate<V> pred) {
    if (size() > a.length) {
      a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size());
    }
    int i = 0;
    Object[] result = a;
    final V[] localStorage = storage;
    for (V v : localStorage) {
      if (v != null && v != DELETED) {
        if (pred.test(v)) {
          if (i < result.length) {
            result[i++] = v;
          } else {
            return (T[]) result;
          }
        }
      }
    }
    if (i < result.length) {
      result[i] = null;
    }
    return (T[]) result;
  }

  /*
   ** Map implementation -- mutators
   */

  public synchronized V put(K key, V value) {
    if (!keyDef.equalKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
    }
    return internalPut(value, NORMAL, null);
  }

  public synchronized void putAll(Map<? extends K, ? extends V> t) {
    // TODO: it's impossible to know which kind of iteration is better: entrySet() or keySet()+get()
    for (K key : t.keySet()) {
      V value = t.get(key);
      if (!keyDef.equalKey(key, value)) {
        throw new IllegalArgumentException(
            "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
      }
      internalPut(value, NORMAL, null);
    }
  }

  public synchronized V putIfAbsent(K key, V value) {
    if (!keyDef.equalKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
    }
    return internalPut(value, IF_ABSENT, null);
  }

  public synchronized V replace(K key, V value) {
    if (!keyDef.equalKey(key, value)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + keyDef.getKey(value));
    }
    return internalPut(value, REPLACE, null);
  }

  public synchronized boolean replace(K key, V oldValue, V newValue) {
    if (!keyDef.equalKey(key, newValue)) {
      throw new IllegalArgumentException(
          "key and value are inconsistent:" + key + " and " + keyDef.getKey(newValue));
    }
    return internalPut(newValue, REPLACE, oldValue) != null;
  }

  public synchronized boolean add(V value) {
    return internalPut(value, NORMAL, null) == null;
  }

  public synchronized boolean addAll(Collection<? extends V> t) {
    boolean result = false;
    for (V v : t) {
      if (internalPut(v, NORMAL, null) == null) {
        result = true;
      }
    }
    return result;
  }

  private static final int NORMAL = 0;
  private static final int IF_ABSENT = 1;
  private static final int REPLACE = 2;

  protected V internalPut(V value, int mode, V oldValue) {
    V[] state = storage;
    K key = keyDef.getKey(value);
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = state[index];

    if (candidate == null) {
      if (mode != REPLACE) {
        state[index] = value;
        _indexableList = null;
        postInsertHook(true);
      }
      return null;
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
          if (mode != REPLACE) {
            state[firstDeleted] = value;
            _indexableList = null;
            postInsertHook(false);
          }
          return null;
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          if (mode != IF_ABSENT) {
            state[index] = value;
            _indexableList = null;
          }
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
      if (mode != IF_ABSENT && (oldValue == null || candidate.equals(oldValue))) {
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
          if (mode != REPLACE) {
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
              if (mode != REPLACE) {
                state[firstDeleted] = value;
                _indexableList = null;
                postInsertHook(false);
              }
              return null;
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              if (mode != IF_ABSENT && (oldValue == null || candidate.equals(oldValue))) {
                state[index] = value;
                _indexableList = null;
              }
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          if (mode != IF_ABSENT && (oldValue == null || candidate.equals(oldValue))) {
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
  public interface ValueFactory<K, V> {
    V newValue(K key);
  }

  /**
   * If values can be constructed from just a key and an int, this interface can be used with
   * putIfAbsent(K, ValueFactory, int) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryI<K, V> {
    V newValue(K key, int extra);
  }

  /**
   * If values can be constructed from just a key and a long, this interface can be used with
   * putIfAbsent(K, ValueFactory, long) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryL<K, V> {
    V newValue(K key, long extra);
  }

  /**
   * If values can be constructed from just a key and a boolean, this interface can be used with
   * putIfAbsent(K, ValueFactory, long) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryB<K, V> {
    V newValue(K key, boolean extra);
  }

  /**
   * If values can be constructed from just a key and two ints, this interface can be used with
   * putIfAbsent(K, ValueFactory, int, int) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryII<K, V> {
    V newValue(K key, int extra, int extra2);
  }

  /**
   * If values can be constructed from just a key, and int and a long, this interface can be used
   * with putIfAbsent(K, ValueFactory, int) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryIL<K, V> {
    V newValue(K key, int extra, long extra2);
  }

  /**
   * If values can be constructed from just a key and an extra object, this interface can be used
   * with putIfAbsent(K, ValueFactory, T) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryT<K, V, T> {
    V newValue(K key, T extra);
  }

  /**
   * If values can be constructed from just a key, an int and an object, this interface can be used
   * with putIfAbsent(K, ValueFactoryIT, int, T) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryIT<K, V, T> {
    V newValue(K key, int extra, T extra2);
  }

  public interface ValueFactoryIIT<K, V, T> {
    V newValue(K key, int extra, int extra2, T extra3);
  }

  /**
   * If values can be constructed from just a key and two extra objects, this interface can be used
   * with putIfAbsent(K, ValueFactory, T1, T2) to implement an atomic find-or-add operation.
   */
  public interface ValueFactoryTT<K, V, T1, T2> {
    V newValue(K key, T1 extra1, T2 extra2);
  }

  /**
   * If values can be constructed from just a key and three extra objects, this interface can be
   * used with putIfAbsent(K, ValueFactory, T1, T2, T3) to implement an atomic find-or-add
   * operation.
   */
  public interface ValueFactoryTTT<K, V, T1, T2, T3> {
    V newValue(K key, T1 extra1, T2 extra2, T3 extra3);
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public V putIfAbsent(K key, ValueFactory<K, V> factory) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsent(key, factory);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public V putIfAbsent(K key, ValueFactoryI<K, V> factory, int extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentI(key, factory, extra);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public V putIfAbsent(K key, ValueFactoryL<K, V> factory, long extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentL(key, factory, extra);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public V putIfAbsent(K key, ValueFactoryB<K, V> factory, boolean extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentB(key, factory, extra);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public <T> V putIfAbsent(K key, ValueFactoryT<K, V, T> factory, T extra) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentT(key, factory, extra);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public V putIfAbsent(K key, ValueFactoryII<K, V> factory, int extra, int extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentII(key, factory, extra, extra2);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public <T> V putIfAbsent(K key, ValueFactoryIT<K, V, T> factory, int extra, T extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentIT(key, factory, extra, extra2);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public <T1, T2> V putIfAbsent(K key, ValueFactoryTT<K, V, T1, T2> factory, T1 extra1, T2 extra2) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentTT(key, factory, extra1, extra2);
      }
    }
    return value;
  }

  /**
   * Creates and adds a new value via the factory, but only if the key is not already present.
   *
   * @return the value, either the new or the previous value. Note that this differs from the
   *     java.util.ConcurrentMap(K k, V v), which returns the existing value or null.
   */
  public <T1, T2, T3> V putIfAbsent(
      K key, ValueFactoryTTT<K, V, T1, T2, T3> factory, T1 extra1, T2 extra2, T3 extra3) {
    V value = get(key);
    if (value == null) {
      synchronized (this) {
        value = internalCreateIfAbsentTTT(key, factory, extra1, extra2, extra3);
      }
    }
    return value;
  }

  private V internalCreateIfAbsent(K key, ValueFactory<K, V> factory) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private void checkSameKey(K key, V new_v) {
    if (!keyDef.equalKey(key, new_v)) {
      throw new RuntimeException(
          "NewValue key and key don't match. key def=" + keyDef.getKey(new_v) + ", key=" + key);
    }
  }

  private V internalCreateIfAbsentI(K key, ValueFactoryI<K, V> factory, int extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentL(K key, ValueFactoryL<K, V> factory, long extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentB(K key, ValueFactoryB<K, V> factory, boolean extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private V internalCreateIfAbsentII(K key, ValueFactoryII<K, V> factory, int extra, int extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentT(K key, ValueFactoryT<K, V, T> factory, T extra) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T> V internalCreateIfAbsentIT(
      K key, ValueFactoryIT<K, V, T> factory, int extra, T extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T1, T2> V internalCreateIfAbsentTT(
      K key, ValueFactoryTT<K, V, T1, T2> factory, T1 extra1, T2 extra2) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  private <T1, T2, T3> V internalCreateIfAbsentTTT(
      K key, ValueFactoryTTT<K, V, T1, T2, T3> factory, T1 extra1, T2 extra2, T3 extra3) {
    V[] state = storage;
    final int length = state.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    } else if (keyDef.equalKey(key, candidate)) {
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
            } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
              return candidate;
            }
          }
        } else if (keyDef.equalKey(key, candidate)) {
          return candidate;
        }
      }
    }
  }

  public synchronized V removeKey(K key) {
    return internalRemove(key, null);
  }

  public synchronized boolean removeValue(V value) {
    return internalRemove(keyDef.getKey(value), null) != null;
  }

  /**
   * Remove entry for key only if currently mapped to given value.
   *
   * @return true, if the entry was removed
   */
  public synchronized boolean remove(Object key, Object value) {
    return internalRemove((K) key, (V) value) != null;
  }

  protected V internalRemove(K key, V value) {
    V prev = null;
    final V[] vs = storage;
    final int length = vs.length;
    final int hash = keyDef.hashKey(key) & 0x7FFFFFFF;

    int probe, index = hash % length;
    V candidate = vs[index];

    if (candidate == null) {
      return null;
    } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
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
        } else if (candidate != DELETED && keyDef.equalKey(key, candidate)) {
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

  public synchronized boolean retainAll(Collection<?> c) {
    KeyedObjectHash<K, V> keepers = new KeyedObjectHash<K, V>(Math.min(size(), c.size()), keyDef);
    for (Object o : c) {
      if (contains(o)) {
        keepers.add((V) o);
      }
    }
    boolean result = keepers.size() != size();
    swap(keepers);
    if (result) {
      _indexableList = null;
    }
    return result;
  }

  public synchronized boolean removeAll(Collection<?> c) {
    boolean result = false;
    for (Object v : c) {
      if (internalRemove(keyDef.getKey((V) v), null) != null) {
        result = true;
      }
    }
    return result;
  }

  public synchronized void clear() {
    super.clear();
    storage = (V[]) new Object[storage.length];
    _indexableList = null;
  }

  /**
   * Determines if this hash is equal to a map, as specified in the interface java.util.Map: any two
   * maps must be considered equal if they contain the same set of mappings, regardless of
   * implementation.
   *
   * @param other the other object (presumably a Map)
   * @return true if the objects are equal
   */
  public boolean mapEquals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Map)) {
      return false;
    }
    Map other_map = (Map) other;
    if (other_map.size() != size()) {
      return false;
    }
    V[] vs = storage;
    for (V v : vs) {
      if (v != null && v != DELETED) {
        if (!v.equals(other_map.get(keyDef.getKey(v)))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns a hash code for the entire hash, as specified in the interface java.util.Map: the hash
   * code of a map is the sum of the hash codes of its entries, and the hash code of an entry is the
   * bitwise-xor of the hashCodes of the entry's key and value.
   *
   * @return the hash code.
   */
  public int mapHashCode() {
    V[] vs = storage;
    int hashCode = 0;
    for (V v : vs) {
      if (v != null && v != DELETED) {
        hashCode += keyDef.getKey(v).hashCode() ^ v.hashCode();
      }
    }
    return hashCode;
  }

  /**
   * Determines if this hash is equal to a set, as specified in the interface java.util.Set: any two
   * sets must be considered equal if they have the same size and contain the same elements,
   * regardless of implementation.
   *
   * @param other the other object (presumably a Set)
   * @return true if the objects are equal
   */
  public boolean setEquals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Set)) {
      return false;
    }
    Set other_set = (Set) other;
    if (other_set.size() != size()) {
      return false;
    }
    V[] vs = storage;
    for (V v : vs) {
      if (v != null && v != DELETED) {
        if (!other_set.contains(v)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns a hash code for the entire set, as specified in the interface java.util.Set: the hash
   * code of a set is the sum of the hash codes of its entries.
   *
   * @return the hash code.
   */
  public int setHashCode() {
    V[] vs = storage;
    int hashCode = 0;
    for (V v : vs) {
      if (v != null && v != DELETED) {
        hashCode += v.hashCode();
      }
    }
    return hashCode;
  }

  /*
   ** IndexableMap implementation
   */
  public synchronized V getByIndex(int index) {
    if (_indexableList == null) {
      _indexableList = new ArrayList<V>(size());
      for (V v : storage) {
        if (isValid(v)) {
          _indexableList.add(v);
        }
      }
    }
    return _indexableList.get(index);
  }
}

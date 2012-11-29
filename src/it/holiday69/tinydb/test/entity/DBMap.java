/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.test.entity;

import it.holiday69.tinydb.jdbm.vo.Key;
import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

/**
 *
 * @author fratuz610
 */
public class DBMap implements NavigableMap<Key,Object>{

  // one file index
  // one file for data
  // -- Open in append mode
  // (one file for journal)
  
  private String _dbName;
  private File _dbFolder;
  
  public DBMap(File dbFolder, String dbName) {
    _dbName = dbName;
    _dbFolder = dbFolder;
  }
  
  
  @Override
  public Entry<Key, Object> lowerEntry(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Key lowerKey(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entry<Key, Object> floorEntry(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Key floorKey(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entry<Key, Object> ceilingEntry(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Key ceilingKey(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entry<Key, Object> higherEntry(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Key higherKey(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entry<Key, Object> firstEntry() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entry<Key, Object> lastEntry() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entry<Key, Object> pollFirstEntry() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Entry<Key, Object> pollLastEntry() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NavigableMap<Key, Object> descendingMap() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NavigableSet<Key> navigableKeySet() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NavigableSet<Key> descendingKeySet() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NavigableMap<Key, Object> subMap(Key k, boolean bln, Key k1, boolean bln1) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NavigableMap<Key, Object> headMap(Key k, boolean bln) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NavigableMap<Key, Object> tailMap(Key k, boolean bln) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SortedMap<Key, Object> subMap(Key k, Key k1) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SortedMap<Key, Object> headMap(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SortedMap<Key, Object> tailMap(Key k) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Comparator<? super Key> comparator() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Key firstKey() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Key lastKey() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<Key> keySet() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Collection<Object> values() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<Entry<Key, Object>> entrySet() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean containsKey(Object o) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean containsValue(Object o) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object get(Object o) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object put(Key k, Object v) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object remove(Object o) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void putAll(Map<? extends Key, ? extends Object> map) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}

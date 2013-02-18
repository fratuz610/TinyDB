/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.tinydb.db.entity.RecordRef;
import it.holiday69.tinydb.jdbm.vo.Key;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 * @author fratuz610
 */
public class DiskSortedMap implements SortedMap<Key, Object> {
  
  private DataManager _dataManager;
  private IndexManager _indexManager;
  private GapManager _gapManager;
  
  public DiskSortedMap(File dbFolder, String dbName) throws IOException {
    
    _dataManager = new DataManager(dbFolder, dbName);
    _indexManager = new IndexManager(dbFolder, dbName);
    _gapManager = new GapManager(dbFolder, dbName);
  }
    
  @Override
  public Comparator<? super Key> comparator() {
    return _indexManager.comparator();
  }

  @Override
  public SortedMap<Key, Object> subMap(Key k, Key k1) {
    SortedMap<Key, RecordRef> subIndex = _indexManager.subMap(k, k1);
    SortedMap<Key, Object> retIndex = new TreeMap<Key, Object>();
    
    for(Key key : subIndex.keySet())
      retIndex.put(key, _dataManager.getRecord(subIndex.get(key)));
    
    return retIndex;
  }

  @Override
  public SortedMap<Key, Object> headMap(Key k) {
    
    SortedMap<Key, RecordRef> subIndex = _indexManager.headMap(k);
    
    SortedMap<Key, Object> retIndex = new TreeMap<Key, Object>();
    
    for(Key key : subIndex.keySet())
      retIndex.put(key, _dataManager.getRecord(subIndex.get(key)));
    
    return retIndex;
  }

  @Override
  public SortedMap<Key, Object> tailMap(Key k) {
    SortedMap<Key, RecordRef> subIndex = _indexManager.tailMap(k);
    
    SortedMap<Key, Object> retIndex = new TreeMap<Key, Object>();
    
    for(Key key : subIndex.keySet())
      retIndex.put(key, _dataManager.getRecord(subIndex.get(key)));
    
    return retIndex;
  }

  @Override
  public Key firstKey() {
    return _indexManager.firstKey();
  }

  @Override
  public Key lastKey() {
    return _indexManager.lastKey();
  }

  @Override
  public Set<Key> keySet() {
    return _indexManager.keySet();
  }

  @Override
  public Collection<Object> values() {
    
    Collection<Object> retList = new LinkedList<Object>();
    
    for(Key key : _indexManager.keySet())
      retList.add(_dataManager.getRecord(_indexManager.get(key)));
    
    return retList;
  }

  @Override
  public Set<Entry<Key, Object>> entrySet() {
    Set<Entry<Key, RecordRef>> indexEntrySet = _indexManager.entrySet();
    
    Set<Entry<Key, Object>> ret = new HashSet<Entry<Key, Object>>();
    
    for(Entry<Key, RecordRef> indexEntry : indexEntrySet)
      ret.add(new AbstractMap.SimpleEntry<Key, Object>(indexEntry.getKey(), _dataManager.getRecord(indexEntry.getValue())));
    
    return ret;
  }

  @Override
  public int size() {
    return _indexManager.size();
  }

  @Override
  public boolean isEmpty() {
    return _indexManager.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    return _indexManager.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    return _indexManager.containsValue(o);
  }

  @Override
  public Object get(Object o) {
    return _dataManager.getRecord(_indexManager.get(o));
  }

  @Override
  public Object put(Key k, Object v) {
    _indexManager.put(k, _dataManager.putRecord(v));
    return null;
  }

  @Override
  public Object remove(Object o) {
    
    // we remove the item from the index
    RecordRef ref = _indexManager.remove(o);
    
    // we add the item to the gap list
    _gapManager.addGap(ref);
    
    return _dataManager.getRecord(ref);
  }

  @Override
  public void putAll(Map<? extends Key, ? extends Object> map) {
    
    for(Key key : map.keySet())
      put(key, map.get(key));

  }

  @Override
  public void clear() {
    _indexManager.clear();
    _dataManager.clear();
    _gapManager.clear();
  }
  
}

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
import java.util.Collection;
import java.util.Comparator;
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
  
  private File _dbFolder;
  
  private TreeMap<Key, RecordRef> _index;
  private DataManager _dataManager;
  private IndexManager _indexManager;
  
  public DiskSortedMap(File dbFolder, String dbName) throws IOException {
    
    _dbFolder = dbFolder;
    
    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(_dbFolder, dbName+".index")));
    try {
      _index = (TreeMap<Key, RecordRef>) ois.readObject();
    } catch(Throwable th) {
      _index = new TreeMap<Key, RecordRef>();
    }
    
    _dataManager = new DataManager(dbFolder, dbName);
  }
    
  @Override
  public Comparator<? super Key> comparator() {
    return _index.comparator();
  }

  @Override
  public SortedMap<Key, Object> subMap(Key k, Key k1) {
    SortedMap<Key, RecordRef> subIndex = _index.subMap(k, k1);
    SortedMap<Key, Object> retIndex = new TreeMap<Key, Object>();
    
    for(Key key : subIndex.keySet())
      retIndex.put(key, _dataManager.getRecord(subIndex.get(key)));
    
    return retIndex;
  }

  @Override
  public SortedMap<Key, Object> headMap(Key k) {
    
    SortedMap<Key, RecordRef> subIndex = _index.headMap(k);
    
    SortedMap<Key, Object> retIndex = new TreeMap<Key, Object>();
    
    for(Key key : subIndex.keySet())
      retIndex.put(key, _dataManager.getRecord(subIndex.get(key)));
    
    return retIndex;
  }

  @Override
  public SortedMap<Key, Object> tailMap(Key k) {
    SortedMap<Key, RecordRef> subIndex = _index.tailMap(k);
    
    SortedMap<Key, Object> retIndex = new TreeMap<Key, Object>();
    
    for(Key key : subIndex.keySet())
      retIndex.put(key, _dataManager.getRecord(subIndex.get(key)));
    
    return retIndex;
  }

  @Override
  public Key firstKey() {
    return _index.firstKey();
  }

  @Override
  public Key lastKey() {
    return _index.lastKey();
  }

  @Override
  public Set<Key> keySet() {
    return _index.keySet();
  }

  @Override
  public Collection<Object> values() {
    
    Collection<Object> retList = new LinkedList<Object>();
    
    for(Key key : _index.keySet())
      retList.add(_dataManager.getRecord(_index.get(key)));
    
    return retList;
  }

  @Override
  public Set<Entry<Key, Object>> entrySet() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int size() {
    return _index.size();
  }

  @Override
  public boolean isEmpty() {
    return _index.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    return _index.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    return _index.containsValue(o);
  }

  @Override
  public Object get(Object o) {
    return _dataManager.getRecord(_index.get(o));
  }

  @Override
  public Object put(Key k, Object v) {
    _index.put(k, _dataManager.putRecord(v));
    return null;
  }

  @Override
  public Object remove(Object o) {
    return _index.remove(o);
  }

  @Override
  public void putAll(Map<? extends Key, ? extends Object> map) {
    
    for(Key key : map.keySet())
      put(key, map.get(key));
  }

  @Override
  public void clear() {
    _index.clear();
  }
  
}

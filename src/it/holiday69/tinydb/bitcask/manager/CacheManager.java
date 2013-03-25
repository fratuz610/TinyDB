/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.manager;

import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.vo.Key;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class CacheManager {
  
  private final Logger _log = Logger.getLogger(CacheManager.class.getSimpleName());
  
  private final BitcaskOptions _options;
  
  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
  
  private final Map<Key, ByteArray> _cacheMap = new HashMap<Key, ByteArray>();
  private final LinkedList<Key> _keyList = new LinkedList<Key>();
  
  private int _cacheSize;
  private String _dbName;
  
  public CacheManager(String dbName, BitcaskOptions options) {
    _options = options;
    _dbName = dbName;
  }
  
  public byte[] get(Key key) {
    
    _lock.readLock().lock();
    
    ByteArray ret = null;
    try {
      ret = _cacheMap.get(key);
    } finally {
      _lock.readLock().unlock();
    }

    if(ret != null)
      return ret.getByteArray();
    else
      return null;
    
  }
  
  public void put(Key key, byte[] data) {
    
    if(data == null)
      throw new NullPointerException("the data byte array cannot be null!!");
    
    //_log.info("DB: '"+_dbName+"' Keylist size: " + _keyList.size() + " map key set size: " + _cacheMap.keySet().size());
    
    _lock.writeLock().lock(); 
    
    try {
      
      if(!_cacheMap.containsKey(key))
        _keyList.add(key);
      
      _cacheMap.put(key, new ByteArray(data));
      _cacheSize += data.length;

      while(_cacheSize > _options.cacheSize) {
        
        //_log.info("Reducing: DB: '"+_dbName+"' Keylist size: " + _keyList.size() + " map key set size: " + _cacheMap.keySet().size());
        
        Key keyToRemove = _keyList.removeFirst();
        ByteArray baToRemove = _cacheMap.get(keyToRemove);
        
        if(baToRemove == null)
          throw new RuntimeException("Null byte array from key: " + keyToRemove);
        
        _cacheSize -= baToRemove.getByteArray().length;
        _cacheMap.remove(keyToRemove);
      }
      
      //_log.info("Cache size: " + _cacheMap.keySet().size() + " / " + _cacheSize + " bytes");
    } finally {
      _lock.writeLock().unlock();
    }
  }
  
  public void delete(Key key) {
    
    _lock.writeLock().lock(); 
    
    try {
      
      if(!_keyList.isEmpty())
        _keyList.remove(key);
      
      ByteArray removeBa = _cacheMap.remove(key);
      
      if(removeBa != null)
        _cacheSize -= removeBa.getByteArray().length;
      _log.info("Cache size: " + _cacheMap.keySet().size() + " / " + _cacheSize + " bytes");
    } finally {
      _lock.writeLock().unlock();
    }
    
  }
  
  public static class ByteArray {
    
    private byte[] _data;
    
    public ByteArray(byte[] data) {
      _data = data;
    }
    
    public byte[] getByteArray() {
      return _data;
    }
    
  }
  
}


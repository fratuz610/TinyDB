/*
    Copyright 2013 Stefano Fratini (mail@stefanofratini.it)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package it.holiday69.tinydb.bitcask.manager;

import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.log.DBLog;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author Stefano
 */
public class CacheManager {
  
  private final DBLog _log = DBLog.getInstance(CacheManager.class.getSimpleName());
  
  private final BitcaskOptions _options;
  
  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
  
  private final Map<Key, ByteArray> _cacheMap = new HashMap<Key, ByteArray>();
  private final LinkedList<Key> _keyList = new LinkedList<Key>();
  
  // cache size is enforced at a class level
  private static AtomicInteger _cacheSize = new AtomicInteger(0);
  
  public CacheManager(BitcaskOptions options) {
    _options = options;
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
      _cacheSize.addAndGet(data.length);

      while(_cacheSize.intValue() > _options.cacheSize && !_keyList.isEmpty()) {
        
        //_log.info("Reducing: DB: '"+_dbName+"' Keylist size: " + _keyList.size() + " map key set size: " + _cacheMap.keySet().size());
        
        Key keyToRemove = _keyList.removeFirst();
        ByteArray baToRemove = _cacheMap.get(keyToRemove);
        
        if(baToRemove == null)
          throw new RuntimeException("Null byte array from key: " + keyToRemove);
        
        _cacheSize.addAndGet(-baToRemove.getByteArray().length);
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
         _cacheSize.addAndGet(-removeBa.getByteArray().length);
      
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



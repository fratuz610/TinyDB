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

package it.holiday69.tinydb.bitcask;

import com.esotericsoftware.kryo.KryoException;
import it.holiday69.tinydb.bitcask.file.DBFileParser;
import it.holiday69.tinydb.bitcask.file.HintFileWriter;
import it.holiday69.tinydb.bitcask.file.utils.DBFileUtils;
import it.holiday69.tinydb.bitcask.manager.AppendManager;
import it.holiday69.tinydb.bitcask.manager.CacheManager;
import it.holiday69.tinydb.bitcask.manager.FileLockManager;
import it.holiday69.tinydb.bitcask.manager.GetManager;
import it.holiday69.tinydb.bitcask.manager.KryoManager;
import it.holiday69.tinydb.bitcask.vo.AppendInfo;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.bitcask.vo.KeyRecord;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class Bitcask implements SortedMap<Key, Object> {
  
  private final Logger _log = Logger.getLogger(Bitcask.class.getSimpleName());
  
  private File _dbFolder;
  private String _dbName;
  private BitcaskOptions _options;
  
  private SortedMap<Key, KeyRecord> _keyRecordMap = new TreeMap<Key, KeyRecord>();
  private Class<? extends Comparable> _keyClass;
  
  private final AppendManager _appendManager;
  private final GetManager _getManager;
  private final CacheManager _cacheManager;
  private final KryoManager _kryoManager;
  
  private final ReadWriteLock _keyMapLock = new ReentrantReadWriteLock();
  private final FileLockManager _fileLockManager = new FileLockManager();
  private ScheduledExecutorService _executor;
  private final ReadCompactDBTask _readCompactDBTask = new ReadCompactDBTask();
  
  public Bitcask(String dbName, BitcaskOptions options, ScheduledExecutorService executor) {
    
    _dbName = dbName;
    _options = options;
    
    _dbFolder = new File(_options.dbFolder);
    _executor = executor;
    
    if(!_dbFolder.exists() && !_dbFolder.mkdir())
      throw new RuntimeException("DB folder: " + _dbFolder + " doesn't exist and cannot be created");
    
    // we run the read autoCompact task at startup
    _readCompactDBTask.run();
        
    // we initialize the append && get manager
    _appendManager = new AppendManager(_dbName, _options, _fileLockManager);
    _getManager = new GetManager(_fileLockManager);
    _cacheManager = new CacheManager(_dbName, _options);
    _kryoManager = new KryoManager(_options);
    
    if(_options.autoCompact ) {
      
      if(_executor == null)
        _executor = Executors.newSingleThreadScheduledExecutor();
      
      _log.fine("Autocompact enabled every: " + _options.compactFrequency + " " + _options.compactTimeUnit);
      
      // we add the shutdown hook to stop the read and autoCompact db task
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

        @Override
        public void run() {
          _log.info("Shutting down executor");
          _executor.shutdownNow();
        }
      }));
      
      // we schedule the read autoCompact db task
      _executor.scheduleWithFixedDelay(_readCompactDBTask, _options.compactFrequency, _options.compactFrequency, _options.compactTimeUnit);
    } else {
      
      _log.fine("Autocompact disabled");
    }
    
    _log.fine("Bitcask '"+dbName+"' started");
    
  }
  
  public Bitcask(String dbName, BitcaskOptions options) {
    this(dbName, options, null);
  }
    
  @Override
  public Comparator<? super Key> comparator() {
    _keyMapLock.readLock().lock();
    try {
      return _keyRecordMap.comparator();
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  @Override
  public SortedMap<Key, Object> subMap(Key fromKey, Key toKey) {
    _keyMapLock.readLock().lock();
    
    TreeMap<Key, Object> ret = new TreeMap<Key, Object>();
    TreeMap<Key, Object> tmp;
    try {
      tmp = new TreeMap<Key, Object>(_keyRecordMap.subMap(fromKey, toKey));
    } finally {
      _keyMapLock.readLock().unlock();
    }
    
    for(Key key : tmp.keySet())
      ret.put(key, get(key));
    
    return ret;
  }

  @Override
  public SortedMap<Key, Object> headMap(Key toKey) {
    _keyMapLock.readLock().lock();
    
    TreeMap<Key, Object> ret = new TreeMap<Key, Object>();
    TreeMap<Key, Object> tmp;
    try {
      tmp = new TreeMap<Key, Object>(_keyRecordMap.headMap(toKey));
    } finally {
      _keyMapLock.readLock().unlock();
    }
    
    for(Key key : tmp.keySet())
      ret.put(key, get(key));
    
    return ret;
  }

  @Override
  public SortedMap<Key, Object> tailMap(Key fromKey) {
    _keyMapLock.readLock().lock();
    
    TreeMap<Key, Object> ret = new TreeMap<Key, Object>();
    TreeMap<Key, Object> tmp;
    try {
      tmp = new TreeMap<Key, Object>(_keyRecordMap.tailMap(fromKey));
    } finally {
      _keyMapLock.readLock().unlock();
    }
    
    for(Key key : tmp.keySet())
      ret.put(key, get(key));
    
    return ret;
  }

  @Override
  public Key firstKey() {
    _keyMapLock.readLock().lock();
    try {
      
      if(!_keyRecordMap.isEmpty())
        return _keyRecordMap.firstKey();
      else
        return null;
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  @Override
  public Key lastKey() {
    _keyMapLock.readLock().lock();
    try {
      if(!_keyRecordMap.isEmpty())
        return _keyRecordMap.lastKey();
      else
        return null;
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  @Override
  public Set<Key> keySet() {
    _keyMapLock.readLock().lock();
    try {
      return new TreeSet<Key>(_keyRecordMap.keySet());
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  @Override
  public Collection<Object> values() {
    
    Set<Key> keySet = keySet();
    
    _log.fine("The keyset for this bitcask has cardinality: " + keySet.size());
    
    Collection<Object> retList = new LinkedList<Object>();
    
    for(Key key : keySet) {
      retList.add(get(key));
    }
    
    _log.fine("All objects retrieved, returning");
    
    return retList;
  }

  @Override
  public Set<Entry<Key, Object>> entrySet() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int size() {
    _keyMapLock.readLock().lock();
    try {
      return _keyRecordMap.size();
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    _keyMapLock.readLock().lock();
    try {
      return _keyRecordMap.isEmpty();
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  @Override
  public boolean containsKey(Object key) {
    _keyMapLock.readLock().lock();
    try {
      return _keyRecordMap.containsKey(key);
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object get(Object key) {
    
    if(key == null)
      throw new NullPointerException("Null key provided!!");
    
    byte[] rawRecord = null;
    
    if(key instanceof String) {
      rawRecord = internalGetRecord(new Key().fromString((String) key));
    } else if(key instanceof Long) {
      rawRecord = internalGetRecord(new Key().fromLong((Long) key));
    } else if(key instanceof Double) {
      rawRecord = internalGetRecord(new Key().fromDouble((Double) key));
    } else if(key instanceof Key) {
      rawRecord = internalGetRecord((Key) key);
    } else
      throw new IllegalArgumentException("Supported key types are only String/Long/Double: " + key.getClass() + " provided");

    if(rawRecord == null) 
      return null;

    try {
      return _kryoManager.deserializeObject(new ByteArrayInputStream(rawRecord));
    } catch(KryoException ex) {
      //_log.info("Unable to deserialize value from key: " + key + " marking the object as deleted");
      return null;
    }
  }
  
  private byte[] internalGetRecord(Key key) {
    
    // write through cache GET
    byte[] cachedData = _cacheManager.get(key);
    
    if(cachedData != null)
      return cachedData;
    
    _keyMapLock.readLock().lock();
    
    try {
      KeyRecord keyRecord = _keyRecordMap.get(key);

      if(keyRecord == null)
        return null;

      _log.finer("internalGetRecord: Trying and retrieving keyRecord: " + keyRecord);

      byte[] retValue = _getManager.retrieveRecord(keyRecord);
      
      // updates the cache
      if(retValue != null)
        _cacheManager.put(key, retValue);
      
      return retValue;
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }
  

  @Override
  public Object put(Key key, Object value) {
    
    if(key.keyValue() instanceof String)
      internalPutRecord(new Key().fromString((String) key.keyValue()), _kryoManager.serializeObject(value));
    else if(key.keyValue() instanceof Long)
      internalPutRecord(new Key().fromLong((Long) key.keyValue()), _kryoManager.serializeObject(value));
    else if(key.keyValue() instanceof Double)
      internalPutRecord(new Key().fromDouble((Double) key.keyValue()), _kryoManager.serializeObject(value));
    else
      throw new IllegalArgumentException("Supported key types are only String/Long/Double");
    
    return value;
  }
  
  private void internalPutRecord(Key key, byte[] data) {
    
    // write through cache PUT
    _cacheManager.put(key, data);
    
    AppendInfo appendInfo = null;
    synchronized(_appendManager) {
      appendInfo = _appendManager.appendRecord(key, data);
    }

    KeyRecord keyRecord = new KeyRecord()
            .withFile(appendInfo.appendFile)
            .withTimestamp(appendInfo.timestamp)
            .withValuePosition(appendInfo.valuePosition)
            .withValueSize(appendInfo.valueSize);

    _log.fine("setting " + key + " linked to " + keyRecord);
    
    _keyMapLock.writeLock().lock();
    try {
      _keyRecordMap.put(key, keyRecord);
    } finally {
      _keyMapLock.writeLock().unlock();
    }
  }
  
  @Override
  public Object remove(Object key) {
    
    // we save an empty record
    
    _keyMapLock.writeLock().lock();
    try {
      Object obj = get(key);
      
      // we write an empty value
      internalPutRecord((Key) key, new byte[0]);
      
      _keyRecordMap.remove(key);
      return obj;
    } finally {
      _keyMapLock.writeLock().unlock();
    }
  }

  @Override
  public void putAll(Map<? extends Key, ? extends Object> m) {
    
    // dumb implementation
    for(Key key : m.keySet()) {
      put(key, m.get(key));
    }
  }

  @Override
  public void clear() {
    _keyMapLock.writeLock().lock();
    try {
      _keyRecordMap.clear();
    } finally {
      _keyMapLock.writeLock().unlock();
    }
  }
  
  public void shutdown(boolean compact) {
    _log.fine("Shutting down bitcask: '" + _dbName + "'");
    
    if(_executor != null)
      _executor.shutdownNow();
    
    if(compact) {
      if(_readCompactDBTask.isRunning()) {
        _log.fine("Final DB compacting not necessary (one already in progress)");
      } else {
        _log.fine("Final DB compacting");
        _readCompactDBTask.run();
      } 
    }
    
    _log.fine("Shutdown complete");
  }

  
  public class ReadCompactDBTask implements Runnable {
    
    private boolean _isRunning = false;

    private Map<Key, KeyRecord> _tempKeyRecordMap = new HashMap<Key, KeyRecord>();
    
    @Override
    public void run() {
      
      try {
        _log.info("ReadCompactDBTask START");
        
        _isRunning = true;

          // we scan the target folder
        File[] dbFileList = DBFileUtils.getDBFileList(_dbFolder, _dbName);
        
        List<File> workingFileList = new LinkedList<File>();
        for(File dbFile : dbFileList) {
          if(!dbFile.equals(_fileLockManager.getAppendFile()))
            workingFileList.add(dbFile);
        }
        
        _log.info("Retrieved " +workingFileList.size() + " db files");

        // if the hint file exists, we parse that first
        File hintFile = DBFileUtils.getHintFile(_dbFolder, _dbName);

        if(hintFile.exists()) {
          
          _log.info("Reading hint file: " + hintFile);

          DBFileParser hintFileParser = new DBFileParser(hintFile);

          _tempKeyRecordMap.putAll(hintFileParser.parseDBFile());

          if(_keyClass == null)
            _keyClass = hintFileParser.getKeyClass();
  
        }
        
        // we read the db files building the keyMap
        for(File dbFile : workingFileList) {
          
          if(!_fileLockManager.tryReadLockFile(dbFile)) {
            _log.info("File " + dbFile + " is an append file in use, skipping it");
            continue;
          }

          try {
            _log.info("Reading db file: " + dbFile);
            
            DBFileParser dbFileParser = new DBFileParser(dbFile);
            
            _tempKeyRecordMap.putAll(dbFileParser.parseDBFile());
            
            if(_keyClass == null)
              _keyClass = dbFileParser.getKeyClass();
            
          } finally {
            _fileLockManager.readUnlockFile(dbFile);
          }
        }
        
        if(workingFileList.isEmpty())  {
          
          // we update the main index and rename the temp hint file at once
          _keyMapLock.writeLock().lock();
          try {
            // we overwrite the old mappings with the new
            
            for(Key key : _tempKeyRecordMap.keySet()) {
              KeyRecord currentKeyRecord = _keyRecordMap.get(key);
              KeyRecord hintKeyRecord = _tempKeyRecordMap.get(key);
              
              // we skip the records that may have been overwritten during the compact operation
              if(currentKeyRecord != null && hintKeyRecord.timestamp < currentKeyRecord.timestamp)
                continue;
              
              _keyRecordMap.put(key, hintKeyRecord);
            }
            
          } finally {
            _keyMapLock.writeLock().unlock();
          }
          
          _log.info("ReadCompactDBTask END");
          return;
        }
        
        // we use the temp hint file location to output all new data
        File tempHintFile = DBFileUtils.getTempHintFile(_dbFolder, _dbName);
        
        long start = new Date().getTime();
        
        // we generate the new hint file and get an update key->record map
        _tempKeyRecordMap = new HintFileWriter(hintFile, tempHintFile, _tempKeyRecordMap, _cacheManager).writeTempHintFile();
        
        long end = new Date().getTime();
        
        _log.info("Temp hint file written in " + (end-start) + " millis");
        
        // we update the main index and rename the temp hint file at once
        _keyMapLock.writeLock().lock();
        try {
          // we overwrite the old mappings with the new
          _keyRecordMap.putAll(_tempKeyRecordMap);
          
          // we delete the old hint file
          hintFile.delete();
          
          // we rename the temporary hint file into the actual one
          tempHintFile.renameTo(hintFile);
        } finally {
          _keyMapLock.writeLock().unlock();
        }

        _log.info("Deleting old files");
                
        // we delete the old files (no longer in use)
        for(File dbFile : workingFileList) {
          
          // actual file deletion
          dbFile.delete();
          
          // waiting for the file to actually been deleted
          while(dbFile.exists()) {
            _log.info("File: " + dbFile + " still exists... waiting...");
            try { Thread.sleep(100); } catch(InterruptedException ex) { return; };
          }
            
        }
        _log.info("ReadCompactDBTask END");
      } catch(Throwable th) {
        _log.warning("Exception while compacting db: " + ExceptionUtils.getFullExceptionInfo(th));
      } finally {
        _isRunning = false;
      }
    
    }
    
    public boolean isRunning() { return _isRunning; } 
    
    
  }
  
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask;

import it.holiday69.tinydb.bitcask.manager.AppendManager;
import it.holiday69.tinydb.bitcask.manager.CacheManager;
import it.holiday69.tinydb.bitcask.file.DBFileParser;
import it.holiday69.tinydb.bitcask.manager.GetManager;
import it.holiday69.tinydb.bitcask.file.HintFileWriter;
import it.holiday69.tinydb.bitcask.file.utils.DBFileUtils;
import it.holiday69.tinydb.bitcask.file.utils.KryoUtils;
import it.holiday69.tinydb.bitcask.vo.AppendInfo;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.bitcask.vo.KeyRecord;
import it.holiday69.tinydb.bitcask.manager.FileLockManager;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collection;
import java.util.Comparator;
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
  
  private final ReadWriteLock _keyMapLock = new ReentrantReadWriteLock();
  private final FileLockManager _fileLockManager = new FileLockManager();
  private final ScheduledExecutorService _compactExecutor;
  private final ReadCompactDBTask _readCompactDBTask = new ReadCompactDBTask();
  
  public Bitcask(String dbName, BitcaskOptions options) {
    
    _dbName = dbName;
    _options = options;
    
    _dbFolder = new File(_options.dbFolder);
    
    if(!_dbFolder.exists() && !_dbFolder.mkdir())
      throw new RuntimeException("DB folder: " + _dbFolder + " doesn't exist and cannot be created");
    
    // we run the read autoCompact task at startup
    _readCompactDBTask.run();
        
    // we initialize the append && get manager
    _appendManager = new AppendManager(_dbName, _options, _fileLockManager);
    _getManager = new GetManager(_fileLockManager);
    _cacheManager = new CacheManager(_dbName, _options);
    
    if(_options.autoCompact) {
      
      _log.fine("Autocompact enabled every: " + _options.compactFrequency + " " + _options.compactTimeUnit);
      
      _compactExecutor = Executors.newSingleThreadScheduledExecutor();
      
      // we add the shutdown hook to stop the read and autoCompact db task
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

        @Override
        public void run() {
          _log.info("Shutting down compact executor");
          _compactExecutor.shutdownNow();
        }
      }));
      
      // we schedule the read autoCompact db task
      _compactExecutor.scheduleWithFixedDelay(_readCompactDBTask, _options.compactFrequency, _options.compactFrequency, _options.compactTimeUnit);
    } else {
      
      _log.fine("Autocompact disabled");
      _compactExecutor = null;
    }
    
    _log.fine("Bitcask '"+dbName+"' started");
    
    
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
    
    if(key instanceof String) {
      byte[] rawRecord = internalGetRecord(new Key().fromString((String) key));
      if(rawRecord == null) return null;
      return KryoUtils.readClassAndObject(new ByteArrayInputStream(rawRecord));
    } else if(key instanceof Long) {
      byte[] rawRecord = internalGetRecord(new Key().fromLong((Long) key));
      if(rawRecord == null) return null;
      return KryoUtils.readClassAndObject(new ByteArrayInputStream(rawRecord));
    } else if(key instanceof Double) {
      byte[] rawRecord = internalGetRecord(new Key().fromDouble((Double) key));
      if(rawRecord == null) return null;
      return KryoUtils.readClassAndObject(new ByteArrayInputStream(rawRecord));
    } else if(key instanceof Key) {
      byte[] rawRecord = internalGetRecord((Key) key);
      if(rawRecord == null) return null;
      return KryoUtils.readClassAndObject(new ByteArrayInputStream(rawRecord));
    } else
      throw new IllegalArgumentException("Supported key types are only String/Long/Double: " + key.getClass() + " provided");
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
      _cacheManager.put(key, retValue);
      
      return retValue;
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }
  

  @Override
  public Object put(Key key, Object value) {
    
    if(key.keyValue() instanceof String)
      internalPutRecord(new Key().fromString((String) key.keyValue()), KryoUtils.writeClassAndObject(value));
    else if(key.keyValue() instanceof Long)
      internalPutRecord(new Key().fromLong((Long) key.keyValue()), KryoUtils.writeClassAndObject(value));
    else if(key.keyValue() instanceof Double)
      internalPutRecord(new Key().fromDouble((Double) key.keyValue()), KryoUtils.writeClassAndObject(value));
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
    
    _keyMapLock.writeLock().lock();
    try {
      Object obj = get(key);
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
    
    _compactExecutor.shutdownNow();
    
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
            _keyRecordMap.putAll(_tempKeyRecordMap);
          } finally {
            _keyMapLock.writeLock().unlock();
          }
          
          _log.info("ReadCompactDBTask END");
          return;
        }
        
        // we use the temp hint file location to output all new data
        File tempHintFile = DBFileUtils.getTempHintFile(_dbFolder, _dbName);
        
        // we generate the new hint file and get an update key->record map
        _tempKeyRecordMap = new HintFileWriter(hintFile, tempHintFile, _tempKeyRecordMap).writeTempHintFile();
        
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

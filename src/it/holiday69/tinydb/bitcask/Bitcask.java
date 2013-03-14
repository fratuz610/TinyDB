/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask;

import it.holiday69.tinydb.bitcask.file.AppendManager;
import it.holiday69.tinydb.bitcask.file.DBFileParser;
import it.holiday69.tinydb.bitcask.file.GetManager;
import it.holiday69.tinydb.bitcask.file.HintFileWriter;
import it.holiday69.tinydb.bitcask.file.keydir.vo.AppendInfo;
import it.holiday69.tinydb.bitcask.file.keydir.vo.Key;
import it.holiday69.tinydb.bitcask.file.keydir.vo.KeyRecord;
import it.holiday69.tinydb.bitcask.file.utils.DBFileUtils;
import it.holiday69.tinydb.bitcask.file.utils.KryoUtils;
import it.holiday69.tinydb.bitcask.lock.FileLockManager;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class Bitcask {
  
  private final Logger _log = Logger.getLogger(Bitcask.class.getSimpleName());
  
  private File _dbFolder;
  private BitcaskOptions _options;
  
  private SortedMap<Key, KeyRecord> _sortedKeyRecordMap = new TreeMap<Key, KeyRecord>();
  private Class<? extends Comparable> _keyClass;
  
  private final AppendManager _appendManager;
  private final GetManager _getManager;
  
  private final ReadWriteLock _keyMapLock = new ReentrantReadWriteLock();
  private final FileLockManager _fileLockManager = new FileLockManager();
  private final ScheduledExecutorService _compactExecutor = Executors.newSingleThreadScheduledExecutor();
  
  public Bitcask(BitcaskOptions options) {
    
    _options = options;
    
    _dbFolder = new File(_options.dbFolder);
    
    if(!_dbFolder.exists() && !_dbFolder.mkdir())
      throw new RuntimeException("DB folder: " + _dbFolder + " doesn't exist and cannot be created");
    
    // we run the read compact task at startup
    new ReadCompactDBTask().run();
        
    // we initialize the append && get manager
    _appendManager = new AppendManager(_options, _fileLockManager);
    _getManager = new GetManager(_fileLockManager);
    
    // we add the shutdown hook to stop the read and compact db task
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        System.out.println("Shutting down compact executor");
        _log.info("Shutting down compact executor");
        _compactExecutor.shutdownNow();
      }
    }));
    
    // we schedule the read compact db task
    _compactExecutor.scheduleWithFixedDelay(new ReadCompactDBTask(), _options.compactFrequency, _options.compactFrequency, _options.compactTimeUnit);
  }
  
  public void addRecord(String keyStr, Object obj) {
    
    if(_keyClass == null)
      _keyClass = String.class;
    
    if(_keyClass != null && _keyClass != String.class)
      throw new RuntimeException("You can only insert " + _keyClass + " keys into this db");
    
    internalAddRecord(new Key().fromString(keyStr), KryoUtils.writeClassAndObject(obj));
  }
  
  public void addRecord(long keyLong, Object obj) {
    
    if(_keyClass == null)
      _keyClass = long.class;
    
    if(_keyClass != null && _keyClass != long.class)
      throw new RuntimeException("You can only insert " + _keyClass + " keys into this db");
    
    internalAddRecord(new Key().fromLong(keyLong), KryoUtils.writeClassAndObject(obj));
  }
  
  public void addRecord(double keyDouble, Object obj) {
    
    if(_keyClass == null)
      _keyClass = double.class;
    
    if(_keyClass != null && _keyClass != double.class)
      throw new RuntimeException("You can only insert " + _keyClass + " keys into this db");
    
    internalAddRecord(new Key().fromDouble(keyDouble), KryoUtils.writeClassAndObject(obj));
  }
  
  private void internalAddRecord(Key key, byte[] data) {
    
    AppendInfo appendInfo = null;
    synchronized(_appendManager) {
      appendInfo = _appendManager.appendRecord(key, data);
    }

    KeyRecord keyRecord = new KeyRecord()
            .withFile(appendInfo.appendFile)
            .withTimestamp(appendInfo.timestamp)
            .withValuePosition(appendInfo.valuePosition)
            .withValueSize(appendInfo.valueSize);

    _log.info("setting " + key + " linked to " + keyRecord);
    
    _keyMapLock.writeLock().lock();
    try {
      _sortedKeyRecordMap.put(key, keyRecord);
    } finally {
      _keyMapLock.writeLock().unlock();
    }
  }
  
  public <T> T getRecord(String strKey, Class<T> classOfT) {
    byte[] rawRecord = internalGetRecord(new Key().fromString(strKey));
    return (T) KryoUtils.readClassAndObject(new ByteArrayInputStream(rawRecord));
  }
  
  public <T> T getRecord(long longKey, Class<T> classOfT) {
    byte[] rawRecord = internalGetRecord(new Key().fromLong(longKey));
    return (T) KryoUtils.readClassAndObject(new ByteArrayInputStream(rawRecord));
  }
  
  public <T> T getRecord(double doubleKey, Class<T> classOfT) {
    byte[] rawRecord = internalGetRecord(new Key().fromDouble(doubleKey));
    return (T) KryoUtils.readClassAndObject(new ByteArrayInputStream(rawRecord));
  }
  
  private byte[] internalGetRecord(Key key) {
    
    _keyMapLock.readLock().lock();
    
    try {
      KeyRecord keyRecord = _sortedKeyRecordMap.get(key);

      if(keyRecord == null)
        return null;

      _log.info("KeyRecord to try: " + keyRecord);

      return _getManager.retrieveRecord(keyRecord);
    } finally {
      _keyMapLock.readLock().unlock();
    }
  }

  
  public class ReadCompactDBTask implements Runnable {

    private Map<Key, KeyRecord> _keyToRecordMap = new HashMap<Key, KeyRecord>();
    
    @Override
    public void run() {
      
      try {
        _log.info("ReadCompactDBTask START");

          // we scan the target folder
        File[] dbFileList = DBFileUtils.getDBFileList(_dbFolder, _options.dbName);
        
        List<File> workingFileList = new LinkedList<File>();
        for(File dbFile : dbFileList) {
          if(!dbFile.equals(_fileLockManager.getAppendFile()))
            workingFileList.add(dbFile);
        }
        
        _log.info("Retrieved " +workingFileList.size() + " db files");

        // if the hint file exists, we parse that first
        File hintFile = DBFileUtils.getHintFile(_dbFolder, _options.dbName);

        if(hintFile.exists()) {
          
          _log.info("Reading hint file: " + hintFile);

          DBFileParser hintFileParser = new DBFileParser(hintFile);

          _keyToRecordMap.putAll(hintFileParser.parseDBFile());

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
            
            _keyToRecordMap.putAll(dbFileParser.parseDBFile());
            
            if(_keyClass == null)
              _keyClass = dbFileParser.getKeyClass();
            
          } finally {
            _fileLockManager.readUnlockFile(dbFile);
          }
        }
        
        // we use the temp hint file location to output all new data
        File tempHintFile = DBFileUtils.getTempHintFile(_dbFolder, _options.dbName);
        
        // we generate the new hint file and get an update key->record map
        _keyToRecordMap = new HintFileWriter(hintFile, tempHintFile, _keyToRecordMap).writeTempHintFile();
        
        // we update the main index and rename the temp hint file at once
        _keyMapLock.writeLock().lock();
        try {
          // we overwrite the old mappings with the new
          _sortedKeyRecordMap.putAll(_keyToRecordMap);
          
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
      } 
    
    }
    
    
  }
  
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask;

import it.holiday69.tinydb.bitcask.file.AppendManager;
import it.holiday69.tinydb.bitcask.file.DBFileParser;
import it.holiday69.tinydb.bitcask.file.GetManager;
import it.holiday69.tinydb.bitcask.file.keydir.vo.Key;
import it.holiday69.tinydb.bitcask.file.keydir.vo.KeyRecord;
import it.holiday69.tinydb.bitcask.file.utils.DBFileUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class Bitcask {
  
  private final Logger _log = Logger.getLogger(Bitcask.class.getSimpleName());
  
  private File _dbFolder;
  private BitcaskOptions _options;
  
  private final Map<Key, KeyRecord> _keyMap = new HashMap<Key, KeyRecord>();
  
  private final AppendManager _appendManager;
  private final GetManager _getManager;
  
  public Bitcask(BitcaskOptions options) {
    
    _options = options;
    
    _dbFolder = new File(_options.dbFolder, _options.dbName);
    
    if(!_dbFolder.exists() && !_dbFolder.mkdir())
      throw new RuntimeException("DB folder: " + _dbFolder + " doesn't exist and cannot be created");
    
    // we scan the target folder
    File[] dbFileList = DBFileUtils.getDBFileList(_dbFolder, _options.dbName);
    File hintFile = DBFileUtils.getHintFile(_dbFolder, _options.dbName);
    
    // if the hint file exists, we parse that first
    if(hintFile.exists())
      _keyMap.putAll(new DBFileParser(hintFile).parseDBFile());
    
    // we read the db files afterwards
    for(File dbFile : dbFileList)
      _keyMap.putAll(new DBFileParser(dbFile).parseDBFile());
        
    // we initialize the append manager
    _appendManager = new AppendManager(_options);
    _getManager = new GetManager();
  }
  
  public void addRecord(String keyStr, byte[] data) {
    addRecord(new Key().fromString(keyStr), data);
  }
  
  public void addRecord(long keyLong, byte[] data) {
    addRecord(new Key().fromLong(keyLong), data);
  }
  
  private void addRecord(Key key, byte[] data) {
    AppendManager.AppendInfo appendInfo = _appendManager.appendRecord(key, data);
    
    KeyRecord keyRecord = new KeyRecord()
            .withFile(appendInfo.appendFile)
            .withTimestamp(appendInfo.timestamp)
            .withValuePosition(appendInfo.valuePosition)
            .withValueSize(appendInfo.valueSize);
    
    if(_keyMap == null)
      throw new RuntimeException("Keymap is null");
    
    _log.info("setting " + key + " linked to " + keyRecord);
    
    _keyMap.put(key, keyRecord);
  }
  
  public byte[] getRecord(String keyStr) {
    return getRecord(new Key().fromString(keyStr));
  }
  
  public byte[] getRecord(long keyLong) {
    return getRecord(new Key().fromLong(keyLong));
  }
  
  private byte[] getRecord(Key key) {
    
    KeyRecord keyRecord = _keyMap.get(key);
    
    if(keyRecord == null)
      return null;
    
    return _getManager.retrieveRecord(keyRecord);
  }
  
}

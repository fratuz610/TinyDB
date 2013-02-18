/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file;

import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.file.keydir.vo.Key;
import it.holiday69.tinydb.bitcask.file.keydir.vo.KeyRecord;
import it.holiday69.tinydb.db.utils.SerialUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class DBFileParser {
  
  private final Logger _log = Logger.getLogger(DBFileParser.class.getSimpleName());
  
  private File _dbFile;
  private int _recordsParsed = 0;
  
  public DBFileParser(File dbFile) {
    
    if(dbFile.exists() || !dbFile.isDirectory())
      throw new RuntimeException("The file "+dbFile+" does not exist!");
    
    _dbFile = dbFile;
  }
  
  public Map<Key, KeyRecord> parseDBFile() {
    
    Map<Key, KeyRecord> retMap = new HashMap<Key, KeyRecord>();
    
    try {
      FileInputStream fis = new FileInputStream(_dbFile);

      KeyRecordWrapper record = null;
      while(fis.available() > 0) {
        record = readRecord(fis);

        if(record == null)
          break;
        
        _recordsParsed++;
        
        retMap.put(record.key, record.keyRecord);
      }
      
      fis.close();
    } catch(IOException ex) {
      _log.info("Unable to parse file: " + _dbFile + " because: " + ex.getMessage());
    }
    
    return retMap;
  }
  
  public KeyRecordWrapper readRecord(InputStream is) {
    
    try {
      
      KeyRecordWrapper ret = new KeyRecordWrapper();
      ret.keyRecord = new KeyRecord().withFile(_dbFile);
      
      byte[] tempBa = new byte[8];
      
      is.read(tempBa);
      long crcValue = SerialUtils.byteArrayToLong(tempBa);
      
      is.read(tempBa);
      long ts = SerialUtils.byteArrayToLong(tempBa);
      
      // we save the timestamp value
      ret.keyRecord.timestamp = ts;
      
      is.read(tempBa);
      long keySize = SerialUtils.byteArrayToLong(tempBa);
      
      is.read(tempBa);
      long valueSize = SerialUtils.byteArrayToLong(tempBa);
      
      // we save the value size
      ret.keyRecord.valueSize = valueSize;
      
      byte[] keyBa = new byte[(int)keySize];
      
      is.read(keyBa);
      
      ret.key = new Key().fromByteArray(keyBa);
      
      ret.keyRecord.valuePosition = _dbFile.length() - is.available();
      
      is.skip(valueSize);
      
      return ret;
      
    } catch(IOException ex) {
      return null;
    }
  }
  
  public int getRecordsParsed() { return _recordsParsed; } 
  
  public static class KeyRecordWrapper {
    public KeyRecord keyRecord;
    public Key key;
  }
  
  
}

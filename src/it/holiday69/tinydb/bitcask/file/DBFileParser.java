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

package it.holiday69.tinydb.bitcask.file;

import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.file.utils.KryoUtils;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.bitcask.vo.KeyRecord;
import it.holiday69.tinydb.log.DBLog;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Stefano
 */
public class DBFileParser {
  
  private final DBLog _log = DBLog.getInstance(DBFileParser.class.getSimpleName());
  
  private File _dbFile;
  private int _recordsParsed = 0;
  private Class<? extends Comparable> _keyClass;
  
  public DBFileParser(File dbFile) {
    
    if(!dbFile.exists())
      throw new IllegalArgumentException("The file "+dbFile+" does not exist!");
    
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
        
        //_log.info("Processed record: " + record.key + " => " + record.keyRecord);
        
        if(retMap.containsKey(record.key)) {
          KeyRecord oldRecord = retMap.get(record.key);
          
          if(oldRecord.timestamp > record.keyRecord.timestamp)
            continue;
        }
        
        if(_keyClass == null)
          _keyClass = record.key.keyValue().getClass();
        
        // empty key records result in 
        if(record.keyRecord.valueSize == 0) {
          retMap.remove(record.key);
          continue;
        }
        
        // we just add the record mapping
        retMap.put(record.key, record.keyRecord);
      }
      
      _log.info("Successfully retrieved " + _recordsParsed + " records");
      
      fis.close();
    } catch(IOException ex) {
      _log.info("Unable to parse file: " + _dbFile + " because: " + ex.getMessage());
    }
    
    return retMap;
  }
  
  private KeyRecordWrapper readRecord(InputStream is) {
    
    try {
      
      KeyRecordWrapper ret = new KeyRecordWrapper();
      ret.keyRecord = new KeyRecord().withFile(_dbFile);
      
      long crcValue = KryoUtils.readLong(is);
      long ts = KryoUtils.readLong(is);
      
      // we save the timestamp value
      ret.keyRecord.timestamp = ts;
      
      // we read the key size (4 bytes);
      int keySize = KryoUtils.readInt(is);
      int valueSize = KryoUtils.readInt(is);
      
      // we save the value size
      ret.keyRecord.valueSize = valueSize;
      
      byte[] keyBa = new byte[(int)keySize];
      
      is.read(keyBa);
      
      ret.key = new Key().fromByteArray(keyBa);
      
      ret.keyRecord.valuePosition = _dbFile.length() - is.available();
      
      is.skip(valueSize);
      
      return ret;
      
    } catch(Throwable th) {
      _log.info("Unable to parse record because: " +  ExceptionUtils.getFullExceptionInfo(th));
      return null;
    }
  }
  
  public int getRecordsParsed() { return _recordsParsed; } 
  
  public Class<? extends Comparable> getKeyClass() { return _keyClass; }
  
  public static class KeyRecordWrapper {
    public KeyRecord keyRecord;
    public Key key;
  }
  
  
}

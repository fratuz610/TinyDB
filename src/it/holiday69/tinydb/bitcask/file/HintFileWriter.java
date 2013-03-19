/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file;

import it.holiday69.tinydb.bitcask.manager.GetManager;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.bitcask.vo.KeyRecord;
import it.holiday69.tinydb.bitcask.vo.Record;
import it.holiday69.tinydb.bitcask.manager.FileLockManager;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class HintFileWriter {
  
  private final Logger _log = Logger.getLogger(HintFileWriter.class.getSimpleName());
  
  private File _hintFile;
  private File _tempHintFile;
  private FileOutputStream _hintFos;
  private long _hintFilePos = 0;
  private long _hintRecordCount = 0;
  
  private Map<Key, KeyRecord> _srcRecordMap;
  
  private final GetManager _getManager;
  
  public HintFileWriter(File hintFile, File tempHintFile, Map<Key, KeyRecord> srcRecordMap) {
    _hintFile = hintFile;
    _tempHintFile = tempHintFile;
    _srcRecordMap = srcRecordMap;
    _getManager = new GetManager();
  }
  
  /**
   * Writes a consolidated hint file (in the temp location) and returns an updated keyRecordMap
   * All records are pointing to the final location hint file though
   * @return the updated keyRecordMap
   */ 
  public Map<Key, KeyRecord> writeTempHintFile() {
    
    Map<Key, KeyRecord> updatedMap = new HashMap<Key, KeyRecord>();
    
    try {
      
      if(_hintFos == null)
        _hintFos = new FileOutputStream(_tempHintFile);
      
      for(Key key : _srcRecordMap.keySet()) {

        KeyRecord keyRecord = _srcRecordMap.get(key);

        //_log.info("Retrieving data from re")
        byte[] data = _getManager.retrieveRecord(keyRecord);

        if(data == null)
          continue;
        
        Record record = new Record(key.toByteArray(), data);

        long valuePosition = _hintFilePos + record.relativeValuePosition();
        long timestamp = new Date().getTime();

        _hintFos.write(record.toByteArray());

        _hintFilePos += record.toByteArray().length;
        _hintRecordCount++;
        
        updatedMap.put(key, new KeyRecord()
                .withFile(_hintFile)
                .withTimestamp(timestamp)
                .withValueSize(record.valueSize)
                .withValuePosition(valuePosition));
      }
      
      _hintFos.flush();
      _hintFos.close();
    } catch(Throwable th) {
      throw new RuntimeException("Unable to append record to file: " + _hintFile + " : ", th);
    }
    
    return updatedMap;
  }
  
}

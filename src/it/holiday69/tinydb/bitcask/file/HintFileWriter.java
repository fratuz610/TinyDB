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

import it.holiday69.tinydb.bitcask.file.utils.SnappyHelper;
import it.holiday69.tinydb.bitcask.manager.CacheManager;
import it.holiday69.tinydb.bitcask.manager.GetManager;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.bitcask.vo.KeyRecord;
import it.holiday69.tinydb.bitcask.vo.Record;
import it.holiday69.tinydb.bitcask.manager.FileLockManager;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.iq80.snappy.Snappy;

/**
 *
 * @author Stefano
 */
public class HintFileWriter {
  
  private final Logger _log = Logger.getLogger(HintFileWriter.class.getSimpleName());
  private final SnappyHelper _snappyHelper = new SnappyHelper();
  
  private File _hintFile;
  private File _tempHintFile;
  private FileOutputStream _hintFos;
  private long _hintFilePos = 0;
  
  private Map<Key, KeyRecord> _srcRecordMap;
  
  private final GetManager _getManager;
  private final CacheManager _cacheManager;
  
  public HintFileWriter(File hintFile, File tempHintFile, Map<Key, KeyRecord> srcRecordMap, CacheManager cacheManager) {
    _hintFile = hintFile;
    _tempHintFile = tempHintFile;
    _srcRecordMap = srcRecordMap;
    _getManager = new GetManager();
    _cacheManager = cacheManager;
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
      
      FileChannel fch = _hintFos.getChannel();
      
      for(Key key : _srcRecordMap.keySet()) {

        KeyRecord keyRecord = _srcRecordMap.get(key);
        
        if(keyRecord.valueSize == 0)
          continue;
        
        byte[] data = null;
        
        // we check the cache first (if available
        if(_cacheManager != null)
          data = _cacheManager.get(key);
        
        // we hit the disk if we need to
        if(data == null)
          data = _getManager.retrieveRecord(keyRecord);

        if(data == null)
          continue;
        
        Record record = new Record(key.toByteArray(), data);

        long valuePosition = _hintFilePos + record.relativeValuePosition();
        long timestamp = new Date().getTime();

        fch.write(ByteBuffer.wrap(record.toByteArray()));
        //_hintFos.write(record.toByteArray());

        _hintFilePos += record.toByteArray().length;
        
        updatedMap.put(key, new KeyRecord()
                .withFile(_hintFile)
                .withTimestamp(timestamp)
                .withValueSize(record.valueSize)
                .withValuePosition(valuePosition));
      }
      
      _hintFos.close();
    } catch(Throwable th) {
      throw new RuntimeException("Unable to append record to file: " + _hintFile + " : ", th);
    }
    
    return updatedMap;
  }
  
}

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

import it.holiday69.tinydb.bitcask.vo.KeyRecord;
import it.holiday69.tinydb.bitcask.manager.FileLockManager;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.FileInputStream;
import java.util.logging.Logger;
import org.iq80.snappy.Snappy;

/**
 *
 * @author Stefano
 */
public class GetManager {
  
  private final Logger _log = Logger.getLogger(GetManager.class.getSimpleName());
  
  private final FileLockManager _fileLockManager;
  
  public GetManager(FileLockManager fileLockManager) {
    _fileLockManager = fileLockManager;
  }
  
  public GetManager() {
    _fileLockManager = null;
  }
  
  public byte[] retrieveRecord(KeyRecord keyRecord) {
    
    try {
      
      if(keyRecord.valueSize == 0)
        return null;
      
      // we lock the file for reading if we can
      if(_fileLockManager != null)
        _fileLockManager.readLockFile(keyRecord.file);
      
      FileInputStream fis = new FileInputStream(keyRecord.file);
      
      byte[] ret = new byte[(int)keyRecord.valueSize];
      
      fis.skip(keyRecord.valuePosition);
      fis.read(ret);
      fis.close();
      
      return Snappy.uncompress(ret, 0, ret.length);
      
    } catch(Throwable th) {
      _log.severe(ExceptionUtils.getFullExceptionInfo(th));
      throw new RuntimeException("Unable to retrieve record from file: " + keyRecord.file + " : ", th);
    } finally {
      // we unlock the file no matter what
      if(_fileLockManager != null)
        _fileLockManager.readUnlockFile(keyRecord.file);
    }
  }
  
}



/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.manager;

import it.holiday69.tinydb.bitcask.vo.KeyRecord;
import it.holiday69.tinydb.bitcask.manager.FileLockManager;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.FileInputStream;
import java.util.logging.Logger;

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
      // we lock the file for reading if we can
      if(_fileLockManager != null)
        _fileLockManager.readLockFile(keyRecord.file);
      
      FileInputStream fis = new FileInputStream(keyRecord.file);
      
      if(keyRecord.valueSize == 0)
        return null;
      
      byte[] ret = new byte[(int)keyRecord.valueSize];
      
      fis.skip(keyRecord.valuePosition);
      fis.read(ret);
      fis.close();
      
      return ret;
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



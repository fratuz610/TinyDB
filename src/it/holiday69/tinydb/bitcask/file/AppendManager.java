/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file;

import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.file.vo.AppendInfo;
import it.holiday69.tinydb.bitcask.file.vo.Key;
import it.holiday69.tinydb.bitcask.file.vo.Record;
import it.holiday69.tinydb.bitcask.file.utils.DBFileUtils;
import it.holiday69.tinydb.bitcask.lock.FileLockManager;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class AppendManager {
  
  private final Logger _log = Logger.getLogger(AppendManager.class.getSimpleName());
  
  private File _dbFolder;
  private String _dbName;
  private File _currentFile;
  private FileOutputStream _currentFos;
  private Integer _currentFileNumber;
  private long _currentFilePos;
  private int _currentRecordNumber;
  
  private BitcaskOptions _options;
  private FileLockManager _fileLockManager;
  
  public AppendManager(String dbName, BitcaskOptions options, FileLockManager fileLockManager) {
    
    _dbName = dbName;
    _options = options;
    _fileLockManager = fileLockManager;
    
    _dbFolder = new File(options.dbFolder);
    
    _log.fine("Db Folder " + _dbFolder);
    
    if(!_dbFolder.exists() && !_dbFolder.mkdir())
      throw new RuntimeException("DB folder: " + _dbFolder + " doesn't exist and cannot be created");
    
    // initialization on the first entry entered
    // nothing to do here
  }
  
  public AppendInfo appendRecord(Key key, byte[] data) {
    
    try {
      Record record = new Record(key.toByteArray(), data);
      
      if(_currentFos == null) {
        
        // we determine the max file number again
        _currentFileNumber = getMaxFileNumber()+1;
        
        _currentFile = DBFileUtils.getDBFile(_dbFolder, _dbName, _currentFileNumber);
        
        if(_currentFile.exists()) {
          _currentFile = DBFileUtils.getDBFile(_dbFolder, _dbName, _currentFileNumber+1);
          _currentFileNumber++;
        }
        
        _currentFilePos = 0;
        _currentRecordNumber = 0;
        
        _currentFos = new FileOutputStream(_currentFile, true);
        _fileLockManager.setAppendFile(_currentFile);
      }
      
      AppendInfo appendInfo = new AppendInfo();
      appendInfo.appendFile = _currentFile;
      appendInfo.keySize = record.keySize;
      appendInfo.valueSize = record.valueSize;
      appendInfo.valuePosition = _currentFilePos + record.relativeValuePosition();
      appendInfo.timestamp = new Date().getTime();
      
      // we lock until the file is available
      _fileLockManager.writeLockFile(_currentFile);
      
      try {
        _currentFos.write(record.toByteArray());
        _currentFos.flush();
      } finally {
        // we unlock the file for writing
        _fileLockManager.writeUnlockFile(_currentFile);
      }
        
      
      _currentFilePos += record.toByteArray().length;
      _currentRecordNumber++;
      
      _log.fine("Appended record with info " + appendInfo);
      
      if(_currentRecordNumber >= _options.recordPerFile) {
        
        _log.fine("Reached the limit of " + _options.recordPerFile + " creating a new file: " + (_currentFileNumber+1));
         
        // we move to another file
        _currentFos.close();
        _currentFos = null;
        
        // no append file in use yet (it will be for the next append)
        _fileLockManager.setAppendFile(null);
      }
      
      return appendInfo;
    
    } catch(Throwable th) {
      throw new RuntimeException("Unable to append record to file: " + _currentFile + " : ", th);
    }
  }
  
  private int getMaxFileNumber() {
    
    // we scan the target folder
    File[] dbFileList = DBFileUtils.getDBFileList(_dbFolder, _dbName);
    
    int maxFileNumber = -1;
    
    for(File file : dbFileList) {
      int fileNumber = DBFileUtils.getDBFileNumber(file);
      if(fileNumber > maxFileNumber)
        maxFileNumber = fileNumber;
    }
    
    return maxFileNumber;
  }
  
  
}

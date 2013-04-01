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

import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.vo.AppendInfo;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.bitcask.vo.Record;
import it.holiday69.tinydb.bitcask.file.utils.DBFileUtils;
import it.holiday69.tinydb.bitcask.manager.FileLockManager;
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
      
      _currentFos.write(record.toByteArray());
      
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
  /*
  public void flush() {
    
    try {
    if(_currentFos != null)
      _currentFos.flush();
    } catch(Throwable th) {
      throw new RuntimeException("Unable to flush on disk to: " + _currentFile, th);
    }
  }*/
  
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

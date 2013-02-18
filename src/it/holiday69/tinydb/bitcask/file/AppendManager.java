/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file;

import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.file.keydir.vo.Record;
import it.holiday69.tinydb.bitcask.file.keydir.vo.Key;
import it.holiday69.tinydb.bitcask.file.utils.DBFileUtils;
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
  private File _currentFile;
  private FileOutputStream _currentFos;
  private Integer _currentFileNumber;
  private long _currentFilePos;
  private int _currentRecordNumber;
  
  private BitcaskOptions _options;
  
  public AppendManager(BitcaskOptions options) {
    _options = options;
    
    _dbFolder = new File(options.dbFolder, options.dbName);
    
    _log.info("Db Folder" + _dbFolder);
    
    if(!_dbFolder.exists() && !_dbFolder.mkdir())
      throw new RuntimeException("DB folder: " + _dbFolder + " doesn't exist and cannot be created");
    
    // we scan the target folder
    File[] dbFileList = DBFileUtils.getDBFileList(_dbFolder, _options.dbName);
    
    // no files found, starting with one brand new
    if(dbFileList.length == 0) {
      _currentFile = new File(_options.dbFolder, _options.dbName + ".db.000");
      _currentFileNumber = 0;
      _currentRecordNumber = 0;
      _currentFilePos = 0;
      return;
    }
    
    // we get the last file and parse it
    _currentFile = dbFileList[dbFileList.length-1];
    
    DBFileParser parser = new DBFileParser(_currentFile);
    parser.parseDBFile();
    
    _currentFileNumber = DBFileUtils.getDBFileNumber(_currentFile);
    _currentRecordNumber = parser.getRecordsParsed();
    
    if(parser.getRecordsParsed() >= _options.recordPerFile) {
      _currentFileNumber++;
      _currentFile = DBFileUtils.getDBFile(_dbFolder, _options.dbName, _currentFileNumber);
      _currentRecordNumber = 0;
      _currentFilePos = 0;
    } 
  }
  
  public AppendInfo appendRecord(Key key, byte[] data) {
    
    try {
      Record record = new Record(key.toByteArray(), data);
      
      if(_currentFos == null)
        _currentFos = new FileOutputStream(_currentFile, true);
      
      AppendInfo appendInfo = new AppendInfo();
      appendInfo.appendFile = _currentFile;
      appendInfo.keySize = record.keySize;
      appendInfo.valueSize = record.valueSize;
      appendInfo.valuePosition = _currentFilePos + record.relativeValuePosition();
      appendInfo.timestamp = new Date().getTime();
      
      _currentFos.write(record.toByteArray());
      _currentFos.flush();
      
      _currentFilePos += record.toByteArray().length;
      _currentRecordNumber++;
      
      if(_currentRecordNumber >= _options.recordPerFile) {
        // we move to another file
        _currentFos.close();
        _currentFos = null;
        _currentFileNumber++;
        _currentFile = DBFileUtils.getDBFile(_dbFolder, _options.dbName, _currentFileNumber);
        _currentFilePos = 0;
      }
      
      return appendInfo;
    
    } catch(Throwable th) {
      throw new RuntimeException("Unable to append record to file: " + _currentFile + " : ", th);
    }
  }
  
  public static class AppendInfo {
    public File appendFile;
    public long keySize;
    public long valueSize;
    public long valuePosition;
    public long timestamp;
  }
}

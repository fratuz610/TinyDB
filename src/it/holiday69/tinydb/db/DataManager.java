/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.holiday69.tinydb.db.entity.RecordRef;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class DataManager {
  
  private final static Logger log = Logger.getAnonymousLogger();
  
  private final int _fileSize = 1024*1024; // 1 mb
  private final int _maxNumFile = 2048; // 2048 files
  
  private File _dbFolder;
  private String _dbName;
  
  private Map<Integer, MappedByteBuffer> _dataBufferMap = new HashMap<Integer, MappedByteBuffer>();
  private Map<Integer, Integer> _dbFileCursorMap = new HashMap<Integer, Integer>();
  private int _lastDbFileIndex = 0;
  
  
  public DataManager(File dbFolder, String dbName) {
    
    if(!dbFolder.exists())
      if(!dbFolder.mkdir())
        throw new RuntimeException("The database folder: " + dbFolder.getAbsolutePath() + " doesn't exist and cannot be created");
    
    _dbFolder = dbFolder;
    _dbName = dbName;
    
    // we determine which is the last file available
    for(_lastDbFileIndex = 0; _lastDbFileIndex < _maxNumFile; _lastDbFileIndex++) {
      File dbFile = getDBFile(_dbFolder, _dbName, _lastDbFileIndex);
      
      if(!dbFile.exists()) {
        if(_lastDbFileIndex > 0)
          _lastDbFileIndex--;
        break;
      }
    }
    
    System.out.println("DataManager: _lastDbFileIndex: " + _lastDbFileIndex);
    
    // we create the memory buffers
    for(int i = 0; i <= _lastDbFileIndex; i++)
      _dataBufferMap.put(i, getCreateBuffer(i));
  }
  
  public Object getRecord(RecordRef ref) {
    
    MappedByteBuffer buffer = _dataBufferMap.get(ref.fileRef);
    
    if(buffer == null) {
      System.out.println("No fileBuffer at index: " + ref.fileRef);
      return null;
    }
      
    byte[] dataBuffer = new byte[ref.size];
    System.out.println("Getting data from file " + ref.fileRef + " offset: " + ref.offset + " and size: " + ref.size);
    buffer.position(ref.offset);
    buffer.get(dataBuffer);
    
    try {
      Kryo kryo = new Kryo();
      Input input = new Input(dataBuffer);
      return kryo.readClassAndObject(input);
    } catch(Throwable ex) {
      System.out.println("Unable to deserialize item: " + ExceptionUtils.getFullExceptionInfo(ex));
      return null;
    }
  }
  
  public RecordRef putRecord(Object obj) {
    
    // we serialize the class
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    Output output = new Output(bout);
    kryo.writeClassAndObject(output, obj);
    
    byte[] data = bout.toByteArray();
    
    //System.out.println("" + data.length + " bytes to write");
    
    RecordRef ref = new RecordRef();
    
    int targetFileNum = _lastDbFileIndex;
    
    int offset = getBufferOffset(targetFileNum);
      
    if(offset + data.length > _fileSize) {
      System.out.println("Moving target file to " + (targetFileNum+1));
      targetFileNum++;
    }
    
    //System.out.println("Selected " + targetFileNum + " as the file to use");
    
    MappedByteBuffer buf = getCreateBuffer(targetFileNum);
    
    ref.fileRef = targetFileNum;
    ref.offset = getBufferOffset(targetFileNum);
    ref.size = data.length;
    
    buf.position(ref.offset);
    buf.put(data);
    
    increaseBufferCursor(targetFileNum, data.length);
    
    return ref;
  }
  
  private MappedByteBuffer getCreateBuffer(int number) {
    
    try {
    
      if(!_dataBufferMap.containsKey(number)) {
        
        File file = getDBFile(_dbFolder, _dbName, number);
        
        MappedByteBuffer buf = new RandomAccessFile(file, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, _fileSize);
        _dataBufferMap.put(number, buf);
        _dbFileCursorMap.put(number, getFileOffsetByte(buf));
        
        if(_lastDbFileIndex < number)
          _lastDbFileIndex = number;
        
       System.out.println("getCreateBuffer: creating MappedByteBuffer: " + file.getAbsolutePath() + " offset: " + _dbFileCursorMap.get(number));
      }
      
      return _dataBufferMap.get(number);
      
    } catch(Throwable th) {
      throw new RuntimeException("Unable to open/create memory mapped file: " + _dbFolder + "/" + _dbName+number+".data because: " + th.getMessage());
    }
  }
  
  private int getFileOffsetByte(MappedByteBuffer buf) {
    
    int lastByte;
    for(lastByte = _fileSize-1; lastByte >=0; lastByte--) {
      if(buf.get(lastByte) !=0)
        return lastByte+1;
    }
    
    return 0;
  }
  
  private File getDBFile(File dbFolder, String dbName, int index) {
    return new File(dbFolder, dbName+index+".data");
  }
  
  private int getBufferOffset(int index) {
    if(!_dbFileCursorMap.containsKey(index))
      throw new RuntimeException("No file opened with index: " + index);
    return _dbFileCursorMap.get(index);
  }
  
  private void increaseBufferCursor(int index, int offset) {
    if(!_dbFileCursorMap.containsKey(index))
      throw new RuntimeException("No file opened with index: " + index);
    _dbFileCursorMap.put(index, _dbFileCursorMap.get(index) + offset); 
  }
  
  private Set<Integer> getBufferKeySet() {
    return _dataBufferMap.keySet();
  }
  
}

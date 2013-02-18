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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class DataManager {
  
  private final static Logger log = Logger.getAnonymousLogger();
  
  private int _fileSizeUnit;
  
  private File _dbFolder;
  private String _dbName;
  
  private final ReentrantReadWriteLock _dataLock = new ReentrantReadWriteLock();
  
  private RandomAccessFile _diskRaf;
  private MappedByteBuffer _diskBuffer;
  private int _diskBufferSize;
  private GapManager _gapManager;
  
  private int _diskBufferOffset = 0;
  
  public DataManager(File dbFolder, String dbName) {
    this(dbFolder, dbName, 1024*1024); // fileSize unit defaults to 1mb
  }
  
  public DataManager(File dbFolder, String dbName, int fileSizeUnit) {
    
    if(!dbFolder.exists())
      if(!dbFolder.mkdir())
        throw new RuntimeException("The database folder: " + dbFolder.getAbsolutePath() + " doesn't exist and cannot be created");
    
    _dbFolder = dbFolder;
    _dbName = dbName;
    
    // we create the disk buffer
    _diskBuffer = getCreateBuffer();
    
    _gapManager = new GapManager(dbFolder, dbName);
  }
  
  public Object getRecord(int offset, int size) {
    
    RecordRef ref = new RecordRef();
    ref.offset = offset;
    ref.size = size;
    return getRecord(ref);
  }
  
  public Object getRecord(RecordRef ref) {
    
    _dataLock.readLock().lock();
    try {

      byte[] dataBuffer = new byte[ref.size];
      System.out.println("Getting data from offset: " + ref.offset + " and size: " + ref.size);
      _diskBuffer.position(ref.offset);
      _diskBuffer.get(dataBuffer);

      Kryo kryo = new Kryo();
      Input input = new Input(dataBuffer);
      return kryo.readClassAndObject(input);
      
    } catch(Throwable th) {
      System.out.println("Unable to deserialize item: " + ExceptionUtils.getFullExceptionInfo(th));
    } finally {
      _dataLock.readLock().unlock();
    }
    
    return null;
    
  }
  
  public RecordRef putRecord(Object obj) {
    return putRecord(obj, -1);
  }
  
  public RecordRef putRecord(Object obj, int targetOffset) {
    
    _dataLock.writeLock().lock();
    RecordRef ref = new RecordRef();
    
    try {
      // we serialize the class
      Kryo kryo = new Kryo();
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      Output output = new Output(bout);
      kryo.writeClassAndObject(output, obj);

      byte[] data = bout.toByteArray();

      System.out.println("" + data.length + " bytes to write");
      
      if(targetOffset != -1) {
        
        if(targetOffset >= _diskBufferOffset)
          throw new RuntimeException("Unable to update a record at offset: " + targetOffset + " because it's past the end of the current db size");
        
        // we save on the offset requested
        ref.offset = targetOffset;
        ref.size = data.length;
        
        MappedByteBuffer updatedBuf = getCreateBuffer(_diskBufferOffset);
        updatedBuf.position(ref.offset);
        updatedBuf.put(data);
        
      } else {
      
        RecordRef gapRef = _gapManager.acquireGap(data.length);

        // we can overwrite
        if(gapRef != null) {
          _diskBuffer.position(ref.offset);
          _diskBuffer.put(data);
          return gapRef;
        }

        // we need to append
        MappedByteBuffer updatedBuf = getCreateBuffer(_diskBufferOffset + data.length);

        ref.offset = _diskBufferOffset;
        ref.size = data.length;

        updatedBuf.position(ref.offset);
        updatedBuf.put(data);

        _diskBufferOffset += data.length;

        System.out.println("New offset: " + _diskBufferOffset);
      }

    } finally {
      _dataLock.writeLock().unlock();
    }
    
    return ref;
  }
  
  public void clear() {
    
    _dataLock.writeLock().lock();
    
    // closes all files
    deleteDiskBuffer();
    
    _dataLock.writeLock().unlock();
  }
  
  private MappedByteBuffer getCreateBuffer() {
    
    File file = getDBFile(_dbFolder, _dbName);
    
    int targetLength = file.length() >= _fileSizeUnit?(int)file.length():_fileSizeUnit;
    
    return getCreateBuffer(targetLength);
  }
  
  private MappedByteBuffer getCreateBuffer(int sizeRequired) {
    
    try {
    
      File file = getDBFile(_dbFolder, _dbName);
      
      int targetSize = (int) file.length();
      
      // if we already have a disk buffer big enough
      if(_diskBuffer != null) {
        
        if(_diskBufferSize >= sizeRequired)
          return _diskBuffer;
        else {
          _diskBuffer.force();
          _diskRaf.getChannel().close();
          _diskRaf.close();
        }
      }
      
      while(sizeRequired > targetSize)
        targetSize += _fileSizeUnit;
      
      System.out.println("Creating a new memory channel with size: " + targetSize);
      
      _diskRaf = new RandomAccessFile(file, "rw");
      _diskBuffer = _diskRaf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, targetSize);
      _diskBufferSize = targetSize;
      _diskBufferOffset = getFileOffsetByte(_diskBuffer);

      return _diskBuffer;
      
    } catch(Throwable th) {
      throw new RuntimeException("Unable to open/create memory mapped file: " + _dbFolder + "/" + _dbName+".data because: " + th.getMessage());
    }
  }
  
  private void deleteDiskBuffer() {
    
    try {
      _diskBuffer.force();
      _diskRaf.getChannel().close();
      _diskRaf.close();

      _diskBuffer = null;
      _diskBufferSize = _fileSizeUnit;
      _diskBufferOffset = 0;
    } catch(Throwable th) {
      throw new RuntimeException("Unable to deleteDiskBuffer because: " + th.getMessage());
    }
  }
  
  private int getFileOffsetByte(MappedByteBuffer buf) {
    
    int lastByte;
    for(lastByte = _fileSizeUnit-1; lastByte >=0; lastByte--) {
      if(buf.get(lastByte) !=0)
        return lastByte+1;
    }
    
    return 0;
  }
  
  private File getDBFile(File dbFolder, String dbName) {
    return new File(dbFolder, dbName+".data");
  }
  
}

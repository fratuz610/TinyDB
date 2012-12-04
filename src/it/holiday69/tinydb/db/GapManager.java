/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.holiday69.tinydb.db.entity.RecordRef;
import it.holiday69.tinydb.jdbm.vo.Key;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class GapManager {
  
  private final static Logger log = Logger.getAnonymousLogger();
  
  private final int _fileSize = 1024*1024; // 1 mb
  
  private List<RecordRef> _gapList;
  
  private final ScheduledExecutorService _bgExecutor = Executors.newSingleThreadScheduledExecutor();
  private MappedByteBuffer _diskBuffer;
         
  private final ReentrantLock _gapListLock = new ReentrantLock();
  
  public GapManager(File dbFolder, String dbName) {
    
    if(!dbFolder.exists())
      if(!dbFolder.mkdir())
        throw new RuntimeException("The database folder: " + dbFolder.getAbsolutePath() + " doesn't exist and cannot be created");
    
    try {
      _diskBuffer = new RandomAccessFile(getGapFile(dbFolder, dbName), "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, _fileSize);
    } catch(Throwable th) {
      throw new RuntimeException("Unable to open/create index file: " + getGapFile(dbFolder, dbName) + " because: " + th.getMessage());
    }
    
    // we read all data into memory
    readDataFromBuffer();
    
    // we schedule all background tasks
    scheduleBackgroundTasks();
    
  }
  
  private File getGapFile(File dbFolder, String dbName) {
    return new File(dbFolder, dbName+".gap");
  }

  
  public RecordRef acquireGap(int minSize) {
    
    _gapListLock.lock();
    RecordRef ret = null;
    try {
      
      for(RecordRef gapRef : _gapList) {
        if(gapRef.size >= minSize) {
          ret = gapRef;
          break;
        }
      }
      
      if(ret != null) {
        
        // we are returning a gap reference, let's update the internal list
        
        _gapList.remove(_gapList.indexOf(ret));
        RecordRef newGapRef = new RecordRef();
        newGapRef.fileRef = ret.fileRef;
        newGapRef.offset = ret.offset + minSize;
        newGapRef.size = ret.size - minSize;
        
        if(newGapRef.size >0)
          _gapList.add(newGapRef);
      }
      
    } finally {
      _gapListLock.unlock();
    }
    
    return ret;
    
  }
  
  public void addGap(RecordRef newGap) {
    
    _gapListLock.lock();
    try {
      _gapList.add(newGap);
    } finally {
      _gapListLock.unlock();
    }
  }
  
  private void scheduleBackgroundTasks() {
    
    _bgExecutor.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          writeDataToBuffer();
        }
      }, 10, 10, TimeUnit.SECONDS);
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        writeDataToBuffer();
        _bgExecutor.shutdown();
      }
    }));
  }
  
  private void readDataFromBuffer() {
    
    
    _gapListLock.lock();
    
    try {
      Kryo kryo = new Kryo();
      Input input = new Input(new ByteBufferInputStream(_diskBuffer));
      _gapList = (List<RecordRef>) kryo.readClassAndObject(input);
    }  finally {
      _gapList = new LinkedList<RecordRef>();
      _gapListLock.unlock();
    }

    
  }
  
  private void writeDataToBuffer() {
    
    System.out.println("Persisting");
    _gapListLock.lock();
    
    try {

      Kryo kryo = new Kryo();
      _diskBuffer.position(0);
      ByteBufferOutputStream bout = new ByteBufferOutputStream(_diskBuffer);
      Output output = new Output(bout);
      
      kryo.writeClassAndObject(output, _gapList);
    } catch(Throwable th) {
      throw new RuntimeException("Unable to write index data to disk because: " + ExceptionUtils.getDisplableExceptionInfo(th));
    } finally {
      _gapListLock.unlock();
    }

  }
}

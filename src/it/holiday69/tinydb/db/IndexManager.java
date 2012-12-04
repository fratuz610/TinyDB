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
public class IndexManager implements SortedMap<Key, RecordRef> {
  
  private final static Logger log = Logger.getAnonymousLogger();
  
  private final int _fileSize = 1024*1024; // 1 mb
  
  private TreeMap<Key, RecordRef> _index;
  private List<RecordRef> _gapList;
  
  private final ScheduledExecutorService _bgExecutor = Executors.newSingleThreadScheduledExecutor();
  private MappedByteBuffer _diskBuffer;
         
  private final ReentrantLock _bufferLock = new ReentrantLock();
  private final ReentrantLock _gapListLock = new ReentrantLock();
  private final ReentrantReadWriteLock _indexLock = new ReentrantReadWriteLock();
  
  public IndexManager(File dbFolder, String dbName) {
    
    if(!dbFolder.exists())
      if(!dbFolder.mkdir())
        throw new RuntimeException("The database folder: " + dbFolder.getAbsolutePath() + " doesn't exist and cannot be created");
    
    try {
      _diskBuffer = new RandomAccessFile(getIndexFile(dbFolder, dbName), "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, _fileSize);
    } catch(Throwable th) {
      throw new RuntimeException("Unable to open/create index file: " + getIndexFile(dbFolder, dbName) + " because: " + th.getMessage());
    }
    
    // we read all data into memory
    readDataFromBuffer();
    
    // we schedule all background tasks
    scheduleBackgroundTasks();
    
  }
  
  private File getIndexFile(File dbFolder, String dbName) {
    return new File(dbFolder, dbName+".index");
  }

  @Override
  public Comparator<? super Key> comparator() {
    return _index.comparator();
  }

  @Override
  public SortedMap<Key, RecordRef> subMap(Key k, Key k1) {
    _indexLock.readLock().lock();
    SortedMap<Key, RecordRef> ret;
    try {
      ret = new TreeMap<Key, RecordRef>(_index.subMap(k, k1)) ;
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public SortedMap<Key, RecordRef> headMap(Key k) {
    _indexLock.readLock().lock();
    
    SortedMap<Key, RecordRef> ret;
    try {
      ret = new TreeMap<Key, RecordRef>(_index.headMap(k));
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public SortedMap<Key, RecordRef> tailMap(Key k) {
    _indexLock.readLock().lock();
    
    SortedMap<Key, RecordRef> ret;
    try {
      ret = new TreeMap<Key, RecordRef>(_index.tailMap(k));
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Key firstKey() {
    _indexLock.readLock().lock();
    Key ret = null;
    try {
      ret = _index.firstKey();
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Key lastKey() {
    _indexLock.readLock().lock();
    Key ret = null;
    try {
      ret = _index.lastKey();
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Set<Key> keySet() {
    
    _indexLock.readLock().lock();
    Set<Key> ret = null;
    try {
      ret = new HashSet<Key>(_index.keySet());
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Collection<RecordRef> values() {
    
    _indexLock.readLock().lock();
    LinkedList<RecordRef> ret = null;
    try {
      ret = new LinkedList<RecordRef>(_index.values());
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Set<Entry<Key, RecordRef>> entrySet() {
    
    _indexLock.readLock().lock();
    Set<Entry<Key, RecordRef>> ret = null;
    try {
      ret = new HashSet<Entry<Key, RecordRef>>(_index.entrySet());
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public int size() {
    _indexLock.readLock().lock();
    int size = _index.size();
    _indexLock.readLock().unlock();
    return size;
  }

  @Override
  public boolean isEmpty() {
    _indexLock.readLock().lock();
    boolean isEmpty = _index.isEmpty();
    _indexLock.readLock().unlock();
    return isEmpty;
  }

  @Override
  public boolean containsKey(Object o) {
    _indexLock.readLock().lock();
    boolean containsKey = _index.containsKey(o);
    _indexLock.readLock().unlock();
    return containsKey;
  }

  @Override
  public boolean containsValue(Object o) {
    _indexLock.readLock().lock();
    boolean containsValue = _index.containsValue(o);
    _indexLock.readLock().unlock();
    return containsValue;
  }

  @Override
  public RecordRef get(Object o) {
    _indexLock.readLock().lock();
    RecordRef ret = null;
    try {
      ret = _index.get(o);
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public RecordRef put(Key k, RecordRef v) {
    
    _indexLock.writeLock().lock();
    RecordRef ret = null;
    try {
      ret = _index.put(k, v);
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public RecordRef remove(Object o) {
    
    _indexLock.writeLock().lock();
    RecordRef ret = null;
    try {
      ret = _index.remove(o);
    } finally {
      _indexLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public void putAll(Map<? extends Key, ? extends RecordRef> map) {
    
    _indexLock.writeLock().lock();
    try {
      _index.putAll(map);
    } finally {
      _indexLock.readLock().unlock();
    }
  }

  @Override
  public void clear() {
    
    _indexLock.writeLock().lock();
    try {
      _index.clear();
    } finally {
      _indexLock.readLock().unlock();
    }
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
    
    _bufferLock.lock();
    _gapListLock.lock();
    _indexLock.writeLock().lock();
    try {
      Kryo kryo = new Kryo();
      Input input = new Input(new ByteBufferInputStream(_diskBuffer));
      _index = (TreeMap<Key, RecordRef>) kryo.readClassAndObject(input);
      _gapList = (List<RecordRef>) kryo.readClassAndObject(input);
    } catch(Throwable th) {
      _index = new TreeMap<Key, RecordRef>();
      _gapList = new LinkedList<RecordRef>();
    } finally {
      _indexLock.writeLock().unlock();
      _bufferLock.unlock();
      _gapListLock.unlock();
    }

    
  }
  
  private void writeDataToBuffer() {
    
    System.out.println("Persisting");
    
    _bufferLock.lock();
    _gapListLock.lock();
    _indexLock.readLock().lock();
    
    try {

      Kryo kryo = new Kryo();
      _diskBuffer.position(0);
      ByteBufferOutputStream bout = new ByteBufferOutputStream(_diskBuffer);
      Output output = new Output(bout);
      
      kryo.writeClassAndObject(output, _index);
      kryo.writeClassAndObject(output, _gapList);
    } catch(Throwable th) {
      throw new RuntimeException("Unable to write index data to disk because: " + ExceptionUtils.getDisplableExceptionInfo(th));
    } finally {
      _indexLock.readLock().unlock();
      _bufferLock.unlock();
      _gapListLock.unlock();
    }

  }
}

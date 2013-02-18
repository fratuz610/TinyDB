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
import it.holiday69.tinydb.db.entity.IndexTreeNode;
import it.holiday69.tinydb.jdbm.vo.Key;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class IndexManager implements SortedMap<Key, RecordRef> {
  
  private final static Logger log = Logger.getAnonymousLogger();
  
  private final ReentrantLock _diskLock = new ReentrantLock();
  private DataManager _indexDataManager;
  
  public IndexManager(File dbFolder, String dbName) {
    
    if(!dbFolder.exists())
      if(!dbFolder.mkdir())
        throw new RuntimeException("The database folder: " + dbFolder.getAbsolutePath() + " doesn't exist and cannot be created");
    
    // 128kb files for the index
    _indexDataManager = new DataManager(dbFolder, dbName+"-index", 128*1024);
  }
  
  @Override
  public Comparator<? super Key> comparator() {
    throw new UnsupportedOperationException("comparator call not implemented");
  }

  private int skew(IndexTreeNode node) {
    
    if(node == null)
      return -1;
    
    if(node.left == -1) 
      return node.offset;
    
    IndexTreeNode leftNode = (IndexTreeNode) _indexDataManager.getRecord(node.left, IndexTreeNode.sizeOnDisk);
    
    if(leftNode.level == node.level) {
      node.left = leftNode.right;
      leftNode.right = node.offset;
      
      // we save the records we've updated
      _indexDataManager.putRecord(node, node.offset);
      _indexDataManager.putRecord(leftNode, leftNode.offset);
      
      return leftNode.offset;
    }
    
    return node.offset;
  }
  
  private int split(IndexTreeNode node) {
    
    if(node == null)
      return -1;
    
    IndexTreeNode rightNode = (IndexTreeNode) _indexDataManager.getRecord(node.right, IndexTreeNode.sizeOnDisk);
    
    if(node.right == -1 || rightNode.right == -1)
      return node.offset;
    
    IndexTreeNode rightRightNode = (IndexTreeNode) _indexDataManager.getRecord(rightNode.right, IndexTreeNode.sizeOnDisk);
    
    if(node.level == rightRightNode.level) {
      node.right = rightNode.left;
      rightNode.left = node.offset;
      rightNode.level++;
      
      // we save the records we've updated
      _indexDataManager.putRecord(node, node.offset);
      _indexDataManager.putRecord(rightNode, rightNode.offset);
      
      return rightNode.offset;
    }
    
    return node.offset;
  }
  
  private int insert(Key key, RecordRef valueRecordRef, int nodeOffset) {
    
    // if offset is -1 we need to use the root value
    
    if(nodeOffset == -1) {
      IndexTreeNode newNode = new IndexTreeNode();
      newNode.level = 1;
      newNode.value = valueRecordRef;
      newNode.key = key;
      RecordRef tmpRef = _indexDataManager.putRecord(newNode);
      newNode.offset = tmpRef.offset;
      _indexDataManager.putRecord(newNode, newNode.offset);
      return newNode.offset;
    }
    
    //if(recordRef.)
      
    
    
    return 0;
  }
  
  
  
  @Override
  public SortedMap<Key, RecordRef> subMap(Key k, Key k1) {
    _inMemoryLock.readLock().lock();
    SortedMap<Key, RecordRef> ret;
    try {
      ret = new TreeMap<Key, RecordRef>(_index.subMap(k, k1)) ;
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public SortedMap<Key, RecordRef> headMap(Key k) {
    _inMemoryLock.readLock().lock();
    
    SortedMap<Key, RecordRef> ret;
    try {
      ret = new TreeMap<Key, RecordRef>(_index.headMap(k));
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public SortedMap<Key, RecordRef> tailMap(Key k) {
    _inMemoryLock.readLock().lock();
    
    SortedMap<Key, RecordRef> ret;
    try {
      ret = new TreeMap<Key, RecordRef>(_index.tailMap(k));
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Key firstKey() {
    _inMemoryLock.readLock().lock();
    Key ret = null;
    try {
      ret = _index.firstKey();
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Key lastKey() {
    _inMemoryLock.readLock().lock();
    Key ret = null;
    try {
      ret = _index.lastKey();
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Set<Key> keySet() {
    
    _inMemoryLock.readLock().lock();
    Set<Key> ret = null;
    try {
      ret = new HashSet<Key>(_index.keySet());
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Collection<RecordRef> values() {
    
    _inMemoryLock.readLock().lock();
    LinkedList<RecordRef> ret = null;
    try {
      ret = new LinkedList<RecordRef>(_index.values());
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public Set<Entry<Key, RecordRef>> entrySet() {
    
    _inMemoryLock.readLock().lock();
    Set<Entry<Key, RecordRef>> ret = null;
    try {
      ret = new HashSet<Entry<Key, RecordRef>>(_index.entrySet());
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public int size() {
    _inMemoryLock.readLock().lock();
    int size = _index.size();
    _inMemoryLock.readLock().unlock();
    return size;
  }

  @Override
  public boolean isEmpty() {
    _inMemoryLock.readLock().lock();
    boolean isEmpty = _index.isEmpty();
    _inMemoryLock.readLock().unlock();
    return isEmpty;
  }

  @Override
  public boolean containsKey(Object o) {
    _inMemoryLock.readLock().lock();
    boolean containsKey = _index.containsKey(o);
    _inMemoryLock.readLock().unlock();
    return containsKey;
  }

  @Override
  public boolean containsValue(Object o) {
    _inMemoryLock.readLock().lock();
    boolean containsValue = _index.containsValue(o);
    _inMemoryLock.readLock().unlock();
    return containsValue;
  }

  @Override
  public RecordRef get(Object o) {
    _inMemoryLock.readLock().lock();
    RecordRef ret = null;
    try {
      ret = _index.get(o);
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public RecordRef put(Key k, RecordRef v) {
    
    _inMemoryLock.writeLock().lock();
    RecordRef ret = null;
    try {
      ret = _index.put(k, v);
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public RecordRef remove(Object o) {
    
    _inMemoryLock.writeLock().lock();
    RecordRef ret = null;
    try {
      ret = _index.remove(o);
    } finally {
      _inMemoryLock.readLock().unlock();
    }
    return ret;
  }

  @Override
  public void putAll(Map<? extends Key, ? extends RecordRef> map) {
    
    _inMemoryLock.writeLock().lock();
    try {
      _index.putAll(map);
    } finally {
      _inMemoryLock.readLock().unlock();
    }
  }

  @Override
  public void clear() {
    
    _inMemoryLock.writeLock().lock();
    try {
      _index.clear();
    } finally {
      _inMemoryLock.readLock().unlock();
    }
  }
  
  private void scheduleBackgroundTasks() {
    
    _bgExecutor.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          writeDataToDisk();
        }
      }, 10, 10, TimeUnit.SECONDS);
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        writeDataToDisk();
        _bgExecutor.shutdown();
      }
    }));
  }
  
  private void readDataFromDisk() {
    
    _diskLock.lock();
    
    _inMemoryLock.writeLock().lock();
    try {
      Kryo kryo = new Kryo();
      Input input = new Input(new ByteBufferInputStream(_diskBuffer));
      _index = (TreeMap<Key, RecordRef>) kryo.readClassAndObject(input);
    } catch(Throwable th) {
      _index = new TreeMap<Key, RecordRef>();
    } finally {
      _inMemoryLock.writeLock().unlock();
      _diskLock.unlock();
    }

    
  }
  
  private void writeDataToDisk() {
    
    System.out.println("Persisting");
    
    _diskLock.lock();
    _inMemoryLock.readLock().lock();
    
    try {

      Kryo kryo = new Kryo();
      _diskBuffer.position(0);
      ByteBufferOutputStream bout = new ByteBufferOutputStream(_diskBuffer);
      Output output = new Output(bout);
      
      kryo.writeClassAndObject(output, _index);
    } catch(Throwable th) {
      throw new RuntimeException("Unable to write index data to disk because: " + ExceptionUtils.getDisplableExceptionInfo(th));
    } finally {
      _inMemoryLock.readLock().unlock();
      _diskLock.unlock();
    }

  }
}

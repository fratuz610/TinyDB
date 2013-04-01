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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author Stefano
 */
public class FileLockManager {
  
  private final Map<File, ReentrantReadWriteLock> _lockMap = new HashMap<File, ReentrantReadWriteLock>();
  
  private final AtomicReference<File> _appendFileRef = new AtomicReference<File>();
  
  public void readLockFile(File file) {
    
    synchronized(_lockMap) {
      
      if(!_lockMap.containsKey(file))
        _lockMap.put(file, new ReentrantReadWriteLock());
      
      _lockMap.get(file).readLock().lock();
    }
  }
  
  public void writeLockFile(File file) {
    
    synchronized(_lockMap) {
      
      if(!_lockMap.containsKey(file))
        _lockMap.put(file, new ReentrantReadWriteLock());
      
      _lockMap.get(file).writeLock().lock();
    }
  }
  
  public boolean tryReadLockFile(File file) {
    
    synchronized(_lockMap) {
      
      if(!_lockMap.containsKey(file))
        _lockMap.put(file, new ReentrantReadWriteLock());
      
      return _lockMap.get(file).readLock().tryLock();
    }
  }
  
  public boolean tryWriteLockFile(File file) {
    
    synchronized(_lockMap) {
      
      if(!_lockMap.containsKey(file))
        _lockMap.put(file, new ReentrantReadWriteLock());
      
      return _lockMap.get(file).writeLock().tryLock();
    }
  }
  
  public void readUnlockFile(File file) {
    
    synchronized(_lockMap) {
      
      if(!_lockMap.containsKey(file))
        throw new IllegalStateException("No lock held for file " + file);
      
      _lockMap.get(file).readLock().unlock();
    }
  }
  
  public void writeUnlockFile(File file) {
    
    synchronized(_lockMap) {
      
      if(!_lockMap.containsKey(file))
        throw new IllegalStateException("No lock held for file " + file);
      
      _lockMap.get(file).writeLock().unlock();
    }
  }
  
  public void setAppendFile(File appendFile) {
    _appendFileRef.set(appendFile);
  }
  
  public File getAppendFile() { 
    return _appendFileRef.get();
  }
}

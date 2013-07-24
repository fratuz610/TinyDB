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

package it.holiday69.tinydb.db;

import it.holiday69.dataservice.DataService;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.bitcask.manager.SerializationManager;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.db.handler.AsyncPutHandler;
import it.holiday69.tinydb.db.handler.DeleteHandler;
import it.holiday69.tinydb.db.handler.GetHandler;
import it.holiday69.tinydb.db.handler.PutHandler;
import it.holiday69.tinydb.db.handler.SyncPutHandler;
import it.holiday69.tinydb.log.DBLog;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of the DataService interface based on the {@link Bitcask} datastore
 * @author fratuz610
 */
public class TinyDBDataService extends DataService {
  
  private final TinyDBMapper _dbMapper;
  private final BitcaskManager _bitcaskManager;
  private final PutHandler _putHandler;
  private final GetHandler _getHandler;
  private final DeleteHandler _deleteHandler;
  
  private final ExecutorService _executor;
  
  public TinyDBDataService(TinyDBOptions tinyDBOptions, ScheduledExecutorService executor) {
    
    DBLog.start(new File(tinyDBOptions.bitcaskOptions.dbFolder));
    
    _executor = executor;
    _bitcaskManager = new BitcaskManager(tinyDBOptions.bitcaskOptions, executor);
    _dbMapper = new TinyDBMapper();
    
    if(tinyDBOptions.asyncUpdates)
      _putHandler = new AsyncPutHandler(_bitcaskManager, _dbMapper, executor);
    else
      _putHandler = new SyncPutHandler(_bitcaskManager, _dbMapper);
    
    _getHandler = new GetHandler(_bitcaskManager, _dbMapper);
    _deleteHandler = new DeleteHandler(_bitcaskManager, _dbMapper);
  }
  
  public TinyDBDataService(ScheduledExecutorService executor) {
    this(new TinyDBOptions(), executor);
  }
  
  public TinyDBDataService(TinyDBOptions options) {
    this(options, Executors.newScheduledThreadPool(options.executorPoolSize, new DBThreadFactory()));
  }
  
  public TinyDBDataService() {
    this(new TinyDBOptions());
  }
  
  public void mapClass(Class<?> clazz, int index) {
    SerializationManager.mapClass(clazz, index);
  }
  
  @Override
  public <T> void put(T object) {
    _putHandler.put(object);
  }

  @Override
  public <T> void putAll(Iterable<T> entities) {
    
    for(T object : entities)
      put(object);
  }
  
  @Override
  public <T, V> T get(V keyValue, Class<T> classOfT) {
    return _getHandler.getFromKey(keyValue, classOfT);
  }
  
  @Override
  public <T> T get(String fieldName, Object fieldValue, Class<T> classOfT) {
    return _getHandler.getFromQuery(new Query().filter(fieldName, fieldValue), classOfT);
  }

  @Override
  public <T> T get(Query query, Class<T> classOfT) {
    return _getHandler.getFromQuery(query, classOfT);
  }
  
  @Override
  public <T> T get(Class<T> classOfT) {
    return _getHandler.getAny(classOfT);
  }
  
  @Override
  public <T> List<T> getList(String fieldName, Object fieldValue, Class<T> classOfT) {
    return _getHandler.getListFromQuery(new Query().filter(fieldName, fieldValue), classOfT);
  }
  
  @Override
  public <T> List<T> getList(Class<T> classOfT) {
    return _getHandler.getAll(classOfT);
  }
  
  @Override
  public <T> List<T> getList(Query query, Class<T> classOfT) {
    return _getHandler.getListFromQuery(query, classOfT);
  }
 
  @Override
  public <T> void delete(T object) {
    _deleteHandler.delete(object);
  }

  @Override
  public <T> void deleteAll(Iterable<T> entities) {
    for(T obj : entities)
      delete(obj);
  }
  
  @Override
  public <T> void deleteAll(Query query, Class<T> classOfT) {
    List<Key> keyList = _getHandler.getKeysFromQuery(query, classOfT);
    for(Key key : keyList)
      _deleteHandler.deleteFromKey(key, classOfT);
  }
  
  @Override
  public <T> void deleteAll(Class<T> classOfT) {
    _deleteHandler.deleteAll(classOfT);
  }
    
  @Override
  public <T> long getResultSetSize(Class<T> classOfT) {
    return _getHandler.getResultSetSize(classOfT);
  }

  @Override
  public <T> long getResultSetSize(String fieldName, Object fieldValue, Class<T> classOfT) {
    return _getHandler.getResultSetSize(new Query().filter(fieldName, fieldValue), classOfT);
  }

  @Override
  public <T> long getResultSetSize(Query query, Class<T> classOfT) {
    return _getHandler.getResultSetSize(query, classOfT);
  }
  
  public void shutdown(boolean compact) {
    synchronized(_executor) {
      _executor.shutdown();
    }
    _bitcaskManager.shutdown(compact);
    _putHandler.shutdown();
    DBLog.shutdown();
  }
  
  private static class DBThreadFactory implements ThreadFactory {

    private final ThreadGroup tg = new ThreadGroup("TinyDB");
    
    private final AtomicInteger _atomicCounter = new AtomicInteger(0);
    
    @Override
    public Thread newThread(Runnable r) {
      return new Thread(tg, r, "TinyDB-" + _atomicCounter.addAndGet(1));
    }
    
  }
}

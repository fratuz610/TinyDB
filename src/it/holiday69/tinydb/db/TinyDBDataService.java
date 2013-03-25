/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.dataservice.DataService;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.db.handler.AsyncPutHandler;
import it.holiday69.tinydb.db.handler.DeleteHandler;
import it.holiday69.tinydb.db.handler.GetHandler;
import it.holiday69.tinydb.db.handler.PutHandler;
import it.holiday69.tinydb.db.handler.SyncPutHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * An implementation of the DataService interface based on the {@link Bitcask} datastore
 * @author fratuz610
 */
public class TinyDBDataService extends DataService {
  
  private static final Logger _log = Logger.getLogger(TinyDBDataService.class.getSimpleName());

  private final TinyDBMapper _dbMapper;
  private final BitcaskManager _bitcaskManager;
  private final PutHandler _putHandler;
  private final GetHandler _getHandler;
  private final DeleteHandler _deleteHandler;
  private final ScheduledExecutorService _executor;
  
  private final Map<Class, Integer> _kryoClassMap = Collections.synchronizedMap(new HashMap<Class, Integer>());
  
  public TinyDBDataService(BitcaskOptions options, ScheduledExecutorService executor) {
    
    _executor = executor;
    
    _bitcaskManager = new BitcaskManager(options, executor);
    _dbMapper = new TinyDBMapper();
    
    if(options.asyncPuts)
      _putHandler = new AsyncPutHandler(_bitcaskManager, _dbMapper, executor);
    else
      _putHandler = new SyncPutHandler(_bitcaskManager, _dbMapper);
    
    _getHandler = new GetHandler(_bitcaskManager, _dbMapper);
    _deleteHandler = new DeleteHandler(_bitcaskManager, _dbMapper);
  }
  
  public TinyDBDataService(ScheduledExecutorService executor) {
    this(new BitcaskOptions(), executor);
  }
  
  public TinyDBDataService(BitcaskOptions options) {
    this(options, Executors.newScheduledThreadPool(5));
  }
  
  public TinyDBDataService() {
    this(new BitcaskOptions(), Executors.newScheduledThreadPool(5));
  }
  
  public void register(Class<?> clazz, int index) {
    _kryoClassMap.put(clazz, index);
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
    _bitcaskManager.shutdown(compact);
    _putHandler.shutdown();
  }
}

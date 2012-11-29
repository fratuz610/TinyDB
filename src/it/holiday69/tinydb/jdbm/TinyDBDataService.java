/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm;

import it.holiday69.dataservice.DataService;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.jdbm.handler.DeleteHandler;
import it.holiday69.tinydb.jdbm.handler.GetHandler;
import it.holiday69.tinydb.jdbm.handler.PutHandler;
import it.holiday69.tinydb.jdbm.vo.Key;
import java.util.List;
import java.util.logging.Logger;
import org.apache.jdbm.DB;

/**
 * An implementation of the DataService interface based on the {@link DB} datastore
 * @author fratuz610
 */
public class TinyDBDataService extends DataService {
  
  private static final Logger log = Logger.getLogger(TinyDBDataService.class.getSimpleName());

  private final DB _db = TinyDB.getInstance();
  
  private final PutHandler _putHandler = new PutHandler();
  private final GetHandler _getHandler = new GetHandler();
  private final DeleteHandler _deleteHandler = new DeleteHandler();
  
  public TinyDBDataService() {
    
  }
  
  @Override
  public <T> void put(T object) {
    _putHandler.putUncommitted(object);
    
    _db.commit();
  }

  @Override
  public <T> void putAll(Iterable<T> entities) {
    
    for(T object : entities)
      _putHandler.putUncommitted(object);
    
    _db.commit();
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
    
    _deleteHandler.deleteUncommitted(object);
    _db.commit();
  }

  @Override
  public <T> void deleteAll(Iterable<T> entities) {
    
    for(T obj : entities)
      _deleteHandler.deleteUncommitted(obj);
    
    _db.commit();
  }
  
  @Override
  public <T> void deleteAll(Query query, Class<T> classOfT) {
    List<Key> keyList = _getHandler.getKeysFromQuery(query, classOfT);
    for(Key key : keyList)
      _deleteHandler.deleteFromKeyUncommitted(key, classOfT);
  }
  
  @Override
  public <T> void deleteAll(Class<T> className) {
    _deleteHandler.deleteAllUncommitted(className);
    _db.commit();
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
 
  /**
   * Performs maintenance tasks on the datastore, compacting and defragging the values
   * @param quick If true the data s compacted only. If false it's also defragged for faster extracting
   */
  public void performMaintenance(boolean quick) {
    synchronized(_db) {
      _db.defrag(!quick);
    }
  }

}

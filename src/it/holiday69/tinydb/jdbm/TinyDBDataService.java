/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm;

import it.holiday69.tinydb.DataService;
import it.holiday69.tinydb.jdbm.handler.DeleteHandler;
import it.holiday69.tinydb.jdbm.handler.GetHandler;
import it.holiday69.tinydb.jdbm.handler.PutHandler;
import it.holiday69.tinydb.query.Query;
import java.util.List;
import java.util.logging.Logger;
import net.kotek.jdbm.DB;

/**
 *
 * @author fratuz610
 */
public class TinyDBDataService extends DataService {
  
  private static final Logger log = Logger.getLogger(TinyDBDataService.class.getSimpleName());

  private DB _db;
  
  private final PutHandler _putHandler = new PutHandler();
  private final GetHandler _getHandler = new GetHandler();
  private final DeleteHandler _deleteHandler = new DeleteHandler();
  
  public TinyDBDataService() {
    _db = TinyDB.getInstance();
  }
  
  @Override
  public <T> boolean put(T object) {
    
    _putHandler.putUncommitted(object);
    
    _db.commit();
    return true;
  }

  @Override
  public <T> boolean putAll(Iterable<T> entities) {
    
    for(T object : entities)
      _putHandler.putUncommitted(object);
    
    _db.commit();
    return true;
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
  public <T> List<T> getList(Class<T> classOfT) {
    return _getHandler.getAll(classOfT);
  }
  
  @Override
  public <T> List<T> getList(String fieldName, Object fieldValue, Class<T> classOfT) {
    return _getHandler.getListFromQuery(new Query().filter(fieldName, fieldValue), classOfT);
  }
  
  @Override
  public <T> List<T> getList(Query query, Class<T> classOfT) {
    return _getHandler.getListFromQuery(query, classOfT);
  }
 
  @Override
  public <T> boolean delete(T object) {
    
    _deleteHandler.deleteUncommitted(object);
    _db.commit();
    return true;
  }

  @Override
  public <T> boolean deleteAll(Iterable<T> entities) {
    
    for(T obj : entities)
      _deleteHandler.deleteUncommitted(obj);
    
    _db.commit();
    return true;
  }
  
  @Override
  public <T> void deleteAll(Class<T> className) {
    _deleteHandler.deleteAllUncommitted(className);
    _db.commit();
  }
  

  @Override
  public <T> long getResultSetSize(Class<T> classOfT) {
    return TinyDBHelper.getCreateDataTreeMap(classOfT).size();
  }

  @Override
  public <T> long getResultSetSize(String fieldName, Object fieldValue, Class<T> classOfT) {
    return getList(fieldName, fieldValue, classOfT).size();
  }

    
}

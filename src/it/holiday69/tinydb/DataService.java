/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb;

import it.holiday69.tinydb.query.Query;
import java.util.List;

/**
 *
 * @author fratuz610
 */
public abstract class DataService {
  
  private Throwable _lastError;
  
  public abstract <T> boolean put(T object);
  
  public abstract <T> boolean putAll(Iterable<T> entities);
  
  public abstract <T, V> T get(V key, Class<T> classOfT);
  
  public abstract <T> T get(String fieldName, Object fieldValue, Class<T> classOfT);
  
  public abstract <T> T get(Query query, Class<T> classOfT);
  
  public abstract <T> List<T> getList(String fieldName, Object fieldValue, Class<T> classOfT);
  
  public abstract <T> List<T> getList(Class<T> classOfT);
  
  public abstract <T> List<T> getList(Query query, Class<T> classOfT);
  
  public abstract <T> boolean delete(T object);
	
	public abstract <T> boolean deleteAll(Iterable<T> entities);
          
  public abstract <T> void deleteAll(Class<T> className);
  
  public abstract <T> long getResultSetSize(Class<T> classOfT);
  
  public abstract <T> long getResultSetSize(String fieldName, Object fieldValue, Class<T> classOfT);
  
  public Throwable getLastError() { return _lastError; }
    
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class BitcaskManager {
  
  private final static Logger _log = Logger.getLogger(BitcaskManager.class.getSimpleName());
  
  private final Map<String, Bitcask> _classDBMap = new HashMap<String, Bitcask>();
  
  private final BitcaskOptions _options;
  
  public BitcaskManager(BitcaskOptions options) {
    _options = options;
  }
  
  public synchronized Bitcask getEntityDB(String entityName) {
    
    if(!_classDBMap.containsKey(entityName))
      _classDBMap.put(entityName, new Bitcask(entityName, _options));

    return _classDBMap.get(entityName);
  }
  
  public Bitcask getEntityDB(Class<?> entityClass) {
    return getEntityDB(entityClass.getClass().getSimpleName());
  }
  
  public Bitcask getIndexDB(String entityName, String indexName) {
    return getEntityDB(entityName + "-" + indexName);
  }
  
  public Bitcask getIndexDB(Class<?> entityClass, String indexName) {
    return getEntityDB(entityClass.getClass().getSimpleName() + "-" + indexName);
  }
  
  
}

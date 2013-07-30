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

import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.manager.SerializationManager;
import it.holiday69.tinydb.log.DBLog;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class BitcaskManager {
  
  private final DBLog _log = DBLog.getInstance(BitcaskManager.class.getSimpleName());
  
  private final Map<String, Bitcask> _classDBMap = new HashMap<String, Bitcask>();
  
  private final BitcaskOptions _options;
  private final ScheduledExecutorService _executor;
  
  public BitcaskManager(BitcaskOptions options, ScheduledExecutorService executor) {
    _options = options;
    _executor = executor;
  }
  
  public Bitcask getEntityDB(String entityName) {
    
    synchronized(_classDBMap) {
      if(!_classDBMap.containsKey(entityName))
        _classDBMap.put(entityName, new Bitcask(entityName, _options, _executor));

      return _classDBMap.get(entityName);
    }
  }
  
  public Bitcask getEntityDB(Class<?> entityClass) {
    return getEntityDB(entityClass.getSimpleName());
  }
  
  public Bitcask getIndexDB(String entityName, String indexName) {
    return getEntityDB(entityName + "-" + indexName);
  }
  
  public Bitcask getIndexDB(Class<?> entityClass, String indexName) {
    return getEntityDB(entityClass.getSimpleName() + "-" + indexName);
  }
  
  public void shutdown(boolean compact) {
    synchronized(_classDBMap) {
      for(String dbName : _classDBMap.keySet()) {
        _log.info("Shutting down: '" + dbName + "'");
        _classDBMap.get(dbName).shutdown(compact);
      }
    }
    
  }
  
}

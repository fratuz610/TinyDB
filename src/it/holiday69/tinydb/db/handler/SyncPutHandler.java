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

package it.holiday69.tinydb.db.handler;

import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.db.BitcaskManager;
import it.holiday69.tinydb.db.TinyDBMapper;
import it.holiday69.tinydb.db.vo.ClassInfo;
import it.holiday69.tinydb.log.DBLog;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class SyncPutHandler implements PutHandler<Object> {
  
  private final DBLog _log = DBLog.getInstance(SyncPutHandler.class.getSimpleName());
  
  private final BitcaskManager _bitcaskManager;
  private final TinyDBMapper _dbMapper;
  
  public SyncPutHandler(BitcaskManager manager, TinyDBMapper dbMapper) {
    _bitcaskManager = manager;
    _dbMapper = dbMapper;
  }
  
  @Override
  public <T> void put(T newObj) {
   
    try {
      internalPut(newObj);
    } catch(Throwable ex) {
      _log.info("Put failed: " + ExceptionUtils.getDisplableExceptionInfo(ex));
    }
  }
  
  private <T> void internalPut(T newObj) {
        
    ClassInfo classInfo = _dbMapper.getClassInfo(newObj.getClass());
    
    Comparable entityKeyVal = _dbMapper.getIDFieldValue(newObj);
    
    // gets/creates the data tree
    Bitcask dataTree = _bitcaskManager.getEntityDB(newObj.getClass());
    
    // checks if the id field is a long with auto increment (automatic if value is zero or null)
    if(entityKeyVal == null && classInfo.idFieldType == ClassInfo.IDFieldType.LONG) {
      
      Key lastKey = dataTree.lastKey();
      
      if(lastKey == null)
        entityKeyVal = 1l;
      else
        entityKeyVal = ((Long)lastKey.keyValue())+1l;
      
      _dbMapper.setIDFieldValue(newObj, entityKeyVal);
    }
    
    Key primaryKey = new Key().fromComparable(entityKeyVal);
    
    // updates the data tree
    dataTree.put(primaryKey, newObj);
    
    _log.fine("Persisting object : " + newObj);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      Bitcask indexTreeMap = _bitcaskManager.getIndexDB(newObj.getClass(), indexedFieldName);
      
      if(TinyDBMapper.getFieldValue(newObj, indexedFieldName) == null)
        throw new RuntimeException("Field " + indexedFieldName + " is indexed, null values are not allowed");
      
      // cycles through all the values of a field (can be one or more)
      for(Comparable fieldValue : TinyDBMapper.getFieldValue(newObj, indexedFieldName)) {
        
        _log.fine("Field: " + indexedFieldName + " value: " + fieldValue);
        
        Key indexKey = new Key().fromComparable(fieldValue);

        if(!indexTreeMap.containsKey(indexKey))
          indexTreeMap.put(indexKey, new TreeSet<Key>());

        TreeSet<Key> linkedKeySet = (TreeSet<Key>) indexTreeMap.get(indexKey);
        
        if(linkedKeySet == null) {
          _log.fine("Unable to retreive a valid index for value '" + fieldValue + "' and entity: '" + newObj.getClass().getSimpleName() + "'");
          continue;
        }
        
        _log.fine("Analyzing indexedField: '" + indexedFieldName + "' => '"+fieldValue+"'. Cardinality so far: " + linkedKeySet.size());

        linkedKeySet.add(primaryKey);

        indexTreeMap.put(indexKey, new TreeSet<Key>(linkedKeySet));
      }
    }
    
  }

  @Override
  public void shutdown() {
      // do nothing
  }
}

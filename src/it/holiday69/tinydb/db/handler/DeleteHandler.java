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
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class DeleteHandler {
  
  private final Logger log = Logger.getLogger(DeleteHandler.class.getSimpleName());
  
  private final BitcaskManager _bitcaskManager;
  private final TinyDBMapper _dbMapper;
  
  public DeleteHandler(BitcaskManager manager, TinyDBMapper dbMapper) {
    _bitcaskManager = manager;
    _dbMapper = dbMapper;
  }
  
  public <T> void delete(T delObj) {
    
    Comparable entityKeyVal = _dbMapper.getIDFieldValue(delObj);
    
    if(entityKeyVal == null)
      throw new RuntimeException("Unable to delete object with null key!");
    
    deleteFromKey(new Key().fromComparable(entityKeyVal), delObj.getClass());
  }
  
  public <T> void deleteAll(Class<T> classOfT) {
    
    _bitcaskManager.getEntityDB(classOfT).clear();
    
    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      log.info("Deleting indexedField: " + indexedFieldName);
      _bitcaskManager.getIndexDB(classOfT, indexedFieldName).clear();
    }
  }
  
  public <T> void deleteFromKey(Key key, Class<T> classOfT) {
    
    // we get the data map
    Bitcask dataMap = _bitcaskManager.getEntityDB(classOfT);
    
    // we remove the data
    T delObj = (T) dataMap.remove(key);
    
    if(delObj == null)
      return;
    
    // we remove all references in the indexes
    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      Bitcask indexTreeMap = _bitcaskManager.getIndexDB(classOfT, indexedFieldName);
      
      for(Comparable fieldValue : TinyDBMapper.getFieldValue(delObj, indexedFieldName)) {
        
        Key indexKey = new Key().fromComparable(fieldValue);

        TreeSet<Key> linkedKeyList = (TreeSet<Key>) indexTreeMap.get(indexKey);

        if(linkedKeyList != null)
          linkedKeyList.remove(key);

        indexTreeMap.put(indexKey, linkedKeyList);
      }
    }
    
  }
}

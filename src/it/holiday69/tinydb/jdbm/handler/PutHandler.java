/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.handler;

import it.holiday69.tinydb.jdbm.DBHelper;
import it.holiday69.tinydb.jdbm.vo.ClassInfo;
import it.holiday69.tinydb.jdbm.vo.Key;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class PutHandler {
  
  private final Logger log = Logger.getLogger(PutHandler.class.getSimpleName());
  
  public <T> void putUncommitted(T newObj) {
    
    ClassInfo classInfo = DBHelper.getClassInfo(newObj.getClass());
    
    Comparable entityKeyVal = DBHelper.getIDFieldValue(newObj);
    
    // gets/creates the data tree
    NavigableMap<Key, Object> dataTree = DBHelper.getCreateDataTreeMap(newObj.getClass());
    
    // checks if the id field is a long with auto increment (automatic if value is zero or null)
    if(entityKeyVal != null && entityKeyVal instanceof Long && (Long) entityKeyVal == 0) {
      Key lastKey = dataTree.lastKey();

      if(lastKey == null)
        entityKeyVal = 1l;
      else
        entityKeyVal = ((Long)lastKey.keyValue)+1l;
      
      DBHelper.setIDFieldValue(newObj, entityKeyVal);
    }
    
    // updates the data tree
    dataTree.put(Key.fromKeyValue(entityKeyVal), newObj);
    
    //log.info("Persisting object : " + newObj);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      SortedMap<Key,TreeSet<Key>> indexTreeMap = DBHelper.getCreateIndexTreeMap(newObj.getClass(), indexedFieldName);
      
      Key indexKey = Key.fromKeyValue(DBHelper.getFieldValue(newObj, indexedFieldName));
      
      if(!indexTreeMap.containsKey(indexKey))
        indexTreeMap.put(indexKey, new TreeSet<Key>());
      
      TreeSet<Key> linkedKeyList = indexTreeMap.get(indexKey);
      
      //log.info("Analyzing indexedField: " + indexedFieldName + " => "+DBHelper.getFieldValue(newObj, indexedFieldName)+" cardinality so far: " + linkedKeyList.size());
      
      linkedKeyList.add(Key.fromKeyValue(entityKeyVal));
      
      indexTreeMap.put(indexKey, new TreeSet<Key>(linkedKeyList));
    }
    
  }
}

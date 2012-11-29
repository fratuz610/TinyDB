/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.handler;

import it.holiday69.tinydb.jdbm.TinyDBHelper;
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
public class DeleteHandler {
  
  private final Logger log = Logger.getLogger(DeleteHandler.class.getSimpleName());
  
  public <T> void deleteUncommitted(T delObj) {
    
    Comparable entityKeyVal = TinyDBHelper.getIDFieldValue(delObj);
    
    deleteFromKeyUncommitted(Key.fromKeyValue(entityKeyVal), delObj.getClass());
  }
  
  public <T> void deleteAllUncommitted(Class<T> classOfT) {
    
    TinyDBHelper.getCreateDataTreeMap(classOfT).clear();
    
    ClassInfo classInfo = TinyDBHelper.getClassInfo(classOfT);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      log.info("Deleting indexedField: " + indexedFieldName);
      TinyDBHelper.getCreateIndexTreeMap(classOfT, indexedFieldName).clear();
    }
  }
  
  public <T> void deleteFromKeyUncommitted(Key key, Class<T> classOfT) {
    
    // we get the data
    NavigableMap<Key, Object> dataMap = TinyDBHelper.getCreateDataTreeMap(classOfT);
    
    T delObj = (T) TinyDBHelper.getCreateDataTreeMap(classOfT).get(key);
    
    // we remove the data
    dataMap.remove(key);
    
    // we remove all references in the indexes
    ClassInfo classInfo = TinyDBHelper.getClassInfo(classOfT);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      SortedMap<Key,TreeSet<Key>> indexTreeMap = TinyDBHelper.getCreateIndexTreeMap(classOfT, indexedFieldName);
      
      Key indexKey = Key.fromKeyValue(TinyDBHelper.getFieldValue(delObj, indexedFieldName));
      
      TreeSet<Key> linkedKeyList = indexTreeMap.get(indexKey);
      
      if(linkedKeyList != null)
        linkedKeyList.remove(key);
      
    }
    
  }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.handler;

import it.holiday69.tinydb.jdbm.TinyDBHelper;
import it.holiday69.tinydb.jdbm.vo.ClassInfo;
import it.holiday69.tinydb.jdbm.vo.Key;
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
    
    // we remove the data
    TinyDBHelper.getCreateDataTreeMap(delObj.getClass()).remove(Key.fromKeyValue(entityKeyVal));
    
    // we remove all references in the indexes
    
    ClassInfo classInfo = TinyDBHelper.getClassInfo(delObj.getClass());
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      log.info("Analyzing indexedField: " + indexedFieldName);
      
      SortedMap<Key,TreeSet<Key>> indexTreeMap = TinyDBHelper.getCreateIndexTreeMap(delObj.getClass(), indexedFieldName);
      
      Key indexKey = Key.fromKeyValue(TinyDBHelper.getFieldValue(delObj, indexedFieldName));
      
      TreeSet<Key> linkedKeyList = indexTreeMap.get(indexKey);
      
      log.info("Removing key to data: " + Key.fromKeyValue(entityKeyVal));
      
      if(linkedKeyList != null)
        linkedKeyList.remove(Key.fromKeyValue(entityKeyVal));
      
    }
    
  }
  
  public <T> void deleteAllUncommitted(Class<T> className) {
    
    TinyDBHelper.getCreateDataTreeMap(className).clear();
    
    ClassInfo classInfo = TinyDBHelper.getClassInfo(className);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      log.info("Deleting indexedField: " + indexedFieldName);
      TinyDBHelper.getCreateIndexTreeMap(className, indexedFieldName).clear();
    }
  }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.handler;

import it.holiday69.tinydb.jdbm.DBHelper;
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
    Comparable entityKeyVal = DBHelper.getIDFieldValue(delObj);
    
    // we remove the data
    DBHelper.getCreateDataTreeMap(delObj.getClass()).remove(Key.fromKeyValue(entityKeyVal));
    
    // we remove all references in the indexes
    
    ClassInfo classInfo = DBHelper.getClassInfo(delObj.getClass());
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      log.info("Analyzing indexedField: " + indexedFieldName);
      
      SortedMap<Key,TreeSet<Key>> indexTreeMap = DBHelper.getCreateIndexTreeMap(delObj.getClass(), indexedFieldName);
      
      Key indexKey = Key.fromKeyValue(DBHelper.getFieldValue(delObj, indexedFieldName));
      
      TreeSet<Key> linkedKeyList = indexTreeMap.get(indexKey);
      
      log.info("Removing key to data: " + Key.fromKeyValue(entityKeyVal));
      
      if(linkedKeyList != null)
        linkedKeyList.remove(Key.fromKeyValue(entityKeyVal));
      
    }
    
  }
  
  public <T> void deleteAllUncommitted(Class<T> className) {
    
    DBHelper.getCreateDataTreeMap(className).clear();
    
    ClassInfo classInfo = DBHelper.getClassInfo(className);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      log.info("Deleting indexedField: " + indexedFieldName);
      DBHelper.getCreateIndexTreeMap(className, indexedFieldName).clear();
    }
  }
}

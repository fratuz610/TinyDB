/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
    
    // we remove all references in the indexes
    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      Bitcask indexTreeMap = _bitcaskManager.getIndexDB(classOfT, indexedFieldName);
      
      Key indexKey = new Key().fromComparable(TinyDBMapper.getFieldValue(delObj, indexedFieldName));
      
      TreeSet<Key> linkedKeyList = (TreeSet<Key>) indexTreeMap.get(indexKey);
      
      if(linkedKeyList != null)
        linkedKeyList.remove(key);
      
      indexTreeMap.put(indexKey, linkedKeyList);
    }
    
  }
}

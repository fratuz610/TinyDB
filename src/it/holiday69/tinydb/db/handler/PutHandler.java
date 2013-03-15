/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.handler;

import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.file.keydir.vo.Key;
import it.holiday69.tinydb.db.BitcaskManager;
import it.holiday69.tinydb.db.TinyDBMapper;
import it.holiday69.tinydb.jdbm.vo.ClassInfo;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class PutHandler {
  
  private final Logger log = Logger.getLogger(PutHandler.class.getSimpleName());
  
  private final BitcaskManager _bitcaskManager;
  private final TinyDBMapper _dbMapper;
  
  public PutHandler(BitcaskManager manager, TinyDBMapper dbMapper) {
    _bitcaskManager = manager;
    _dbMapper = dbMapper;
  }
  
  public <T> void put(T newObj) {
    
    ClassInfo classInfo = _dbMapper.getClassInfo(newObj.getClass());
    
    Comparable entityKeyVal = _dbMapper.getIDFieldValue(newObj);
    
    // gets/creates the data tree
    Bitcask dataTree = _bitcaskManager.getEntityDB(newObj.getClass());
    
    // checks if the id field is a long with auto increment (automatic if value is zero or null)
    if(entityKeyVal != null && entityKeyVal instanceof Long && (Long) entityKeyVal == 0) {
      Key lastKey = dataTree.lastKey();

      if(lastKey == null)
        entityKeyVal = 1l;
      else
        entityKeyVal = ((Long)lastKey.keyValue())+1l;
      
      _dbMapper.setIDFieldValue(newObj, entityKeyVal);
    }
    
    // updates the data tree
    dataTree.put(new Key().fromComparable(entityKeyVal), newObj);
    
    //log.info("Persisting object : " + newObj);
    
    // updates all the index trees
    for(String indexedFieldName : classInfo.indexedFieldNameList) {
      
      Bitcask indexTreeMap = _bitcaskManager.getIndexDB(newObj.getClass(), indexedFieldName);
      
      Key indexKey = new Key().fromComparable(TinyDBMapper.getFieldValue(newObj, indexedFieldName));
      
      if(!indexTreeMap.containsKey(indexKey))
        indexTreeMap.put(indexKey, new TreeSet<Key>());
      
      TreeSet<Key> linkedKeyList = (TreeSet<Key>) indexTreeMap.get(indexKey);
      
      //log.info("Analyzing indexedField: " + indexedFieldName + " => "+DBHelper.getFieldValue(newObj, indexedFieldName)+" cardinality so far: " + linkedKeyList.size());
      
      linkedKeyList.add(new Key().fromComparable(entityKeyVal));
      
      indexTreeMap.put(indexKey, new TreeSet<Key>(linkedKeyList));
    }
    
  }
}

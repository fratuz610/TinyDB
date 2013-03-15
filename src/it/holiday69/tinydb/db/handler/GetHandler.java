/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.handler;

import it.holiday69.dataservice.query.FieldFilter;
import it.holiday69.dataservice.query.OrderFilter;
import it.holiday69.dataservice.query.OrderType;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.file.keydir.vo.Key;
import it.holiday69.tinydb.db.BitcaskManager;
import it.holiday69.tinydb.db.TinyDBMapper;
import it.holiday69.tinydb.jdbm.vo.ClassInfo;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class GetHandler {
  
  private final Logger log = Logger.getLogger(GetHandler.class.getSimpleName());
  
  private final BitcaskManager _bitcaskManager;
  private final TinyDBMapper _dbMapper;
  
  public GetHandler(BitcaskManager manager, TinyDBMapper dbMapper) {
    _bitcaskManager = manager;
    _dbMapper = dbMapper;
  }
  
  public <T, V> T getFromKey(V keyValue, Class<T> classOfT) {
    
    Key key = new Key().fromComparable((Comparable) keyValue);
    
    return (T) _bitcaskManager.getEntityDB(classOfT).get(key);
  }
  
  public <T> List<T> getAll(Class<T> classOfT) {
    
    Collection<T> values = (Collection<T>) _bitcaskManager.getEntityDB(classOfT).values();
    if(values != null)
      return new LinkedList<T>(values);
    else
      return new LinkedList<T>();
  }
  
  public <T> T getAny(Class<T> classOfT) {
    
    Bitcask db = _bitcaskManager.getEntityDB(classOfT);
    Key firstKey = db.firstKey();
    
    if(firstKey == null)
      return null;
    
    return (T) db.get(firstKey);
  }
  
  public <T> T getFromQuery(Query query, Class<T> classOfT) {
    
    List<Key> finalKeyList = getKeysFromQuery(query, classOfT);
    
    Bitcask db = _bitcaskManager.getEntityDB(classOfT);
    
    for(Key dataKey : finalKeyList) {
     T value = (T) db.get(dataKey);
      if(value != null)
        return value;
      else
        log.warning("There is no value for data index: " + dataKey);
    }
    return null;
  }
  
  public <T> List<T> getListFromQuery(Query query, Class<T> classOfT) {
    
    List<Key> finalKeyList = getKeysFromQuery(query, classOfT);
    
    List<T> finalRetList = new LinkedList<T>();
    
    Bitcask db = _bitcaskManager.getEntityDB(classOfT);
    
    for(Key dataKey : finalKeyList) {
     T value = (T) db.get(dataKey);
      if(value != null)
        finalRetList.add(value);
      else
        log.warning("There is no value for data index: " + dataKey);
    }
    
    return finalRetList;
  }
  
  public <T> List<Key> getKeysFromQuery(Query query, Class<T> classOfT) {
    
    List<Key> finalKeyList = new LinkedList<Key>();
    
    // we apply field filters
    for(FieldFilter fieldFilter : query.getFieldFilterList()) {
      
      if(finalKeyList.isEmpty())
        finalKeyList.addAll(applyFilter(fieldFilter, classOfT));
      else
        finalKeyList.retainAll(applyFilter(fieldFilter, classOfT));
    }
        
    if(query.getOrderFilterList().size() > 1)
      throw new RuntimeException("Multiple order filters are not supported at the moment, please use just one");
    
    if(!query.getOrderFilterList().isEmpty())
      finalKeyList = applyOrder(query.getOrderFilterList().get(0), finalKeyList, classOfT);
    
    // we apply the offset if any
    if(query.getOffset() < finalKeyList.size())
      finalKeyList = finalKeyList.subList(query.getOffset(), finalKeyList.size());
    
    // we apply the limit if any
    if(query.getLimit() <= finalKeyList.size())
      finalKeyList = finalKeyList.subList(0, query.getLimit());
    
    return finalKeyList;
  }
  
  private <T> Set<Key> applyFilter(FieldFilter fieldFilter, Class<T> classOfT) {
    
    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    Set<Key> extractKeyList = new TreeSet<Key>();
    
    // we make sure we don't query for fields that are not indexed
    if(!classInfo.indexedFieldNameList.contains(fieldFilter.getFieldName()))
      throw new RuntimeException("The field: '" + fieldFilter.getFieldName() + "' is not indexed and cannot be used in a query");
    
    Key fieldValueKey = new Key().fromComparable((Comparable) fieldFilter.getFieldValue());
    
    Bitcask indexDB = _bitcaskManager.getIndexDB(classOfT, fieldFilter.getFieldName());
    
    SortedMap<Key,Object> subsetIndexTreeMap = new TreeMap<Key, Object>();
   
    boolean inclusive = false;
    switch(fieldFilter.getFieldFilterType()) {
      case EQUAL: 
        inclusive = true;
        break;
      case GREATER_THAN:
        subsetIndexTreeMap = indexDB.tailMap(fieldValueKey);
        subsetIndexTreeMap.remove(fieldValueKey);
        break;
      case GREATER_THAN_INC:
        inclusive = true;
        subsetIndexTreeMap = indexDB.tailMap(fieldValueKey);
        break;
      case LOWER_THAN:
        subsetIndexTreeMap = indexDB.headMap(fieldValueKey);
        break;
      case LOWER_THAN_INC:
        inclusive = true;
        subsetIndexTreeMap = indexDB.headMap(fieldValueKey);
        break;
    }
    
    // flattens the structure
    for(Key key : subsetIndexTreeMap.keySet())
      extractKeyList.addAll((TreeSet<Key>) subsetIndexTreeMap.get(key));
    
    // adds the inclusive members if necessary
    if(inclusive && indexDB.containsKey(fieldValueKey))
      extractKeyList.addAll((TreeSet<Key>) indexDB.get(fieldValueKey));
    
    return extractKeyList;
  }
  
  
  private <T> List<Key> applyOrder(OrderFilter orderFilter, List<Key> filteredKeyList, Class<T> classOfT) {
    
    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    List<Key> extractKeyList = new LinkedList<Key>();
    
    // we make sure we don't order for fields that are NOT indexed
    if(!classInfo.indexedFieldNameList.contains(orderFilter.getFieldName()))
      throw new RuntimeException("The field: '" + orderFilter.getFieldName() + "' is not indexed and cannot be used in a query");
    
    Bitcask indexTreeMap = _bitcaskManager.getIndexDB(classOfT, orderFilter.getFieldName());
    
    // we flatten the index structure
    for(Object tempTreeSetObj : indexTreeMap.values()) {
      TreeSet<Key> tempTreeSet = (TreeSet<Key>) tempTreeSetObj;
      extractKeyList.addAll(tempTreeSet);
    }
    
    extractKeyList.retainAll(filteredKeyList);
    
    if(orderFilter.getOrderType() == OrderType.DESCENDING)
      Collections.reverse(extractKeyList);
    
    return extractKeyList;
    
  }
  
  public <T> long getResultSetSize(Class<T> classOfT) {
    return _bitcaskManager.getEntityDB(classOfT).keySet().size();
  }

  public <T> long getResultSetSize(Query query, Class<T> classOfT) {
    return getKeysFromQuery(query, classOfT).size();
  }
}

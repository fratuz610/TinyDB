/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.handler;

import it.holiday69.tinydb.query.Query;
import it.holiday69.tinydb.query.OrderFilter;
import it.holiday69.tinydb.query.FieldFilter;
import it.holiday69.tinydb.query.OrderType;
import it.holiday69.tinydb.jdbm.DBHelper;
import it.holiday69.tinydb.jdbm.vo.ClassInfo;
import it.holiday69.tinydb.jdbm.vo.Key;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class GetHandler {
  
  private final Logger log = Logger.getLogger(GetHandler.class.getSimpleName());
  
  public <T, V> T getFromKey(V keyValue, Class<T> classOfT) {
    
    Key key = Key.fromKeyValue((Comparable) keyValue);
    
    return (T) DBHelper.getCreateDataTreeMap(classOfT).get(key);
  }
  
  public <T> List<T> getAll(Class<T> classOfT) {
    
    Collection<T> values = (Collection<T>) DBHelper.getCreateDataTreeMap(classOfT).values();
    if(values != null)
      return new LinkedList<T>(values);
    else
      return new LinkedList<T>();
  }
  
  public <T> T getFromQuery(Query query, Class<T> classOfT) {
    
    Set<Key> finalKeySet = getKeysFromQuery(query, classOfT);
    
    NavigableMap<Key, Object> dataMap = DBHelper.getCreateDataTreeMap(classOfT);
    
    for(Key dataKey : finalKeySet) {
     T value = (T) dataMap.get(dataKey);
      if(value != null)
        return value;
      else
        log.warning("There is no value for data index: " + dataKey);
    }
    
    return null;
  }
  
  public <T> List<T> getListFromQuery(Query query, Class<T> classOfT) {
    
    Set<Key> finalKeySet = getKeysFromQuery(query, classOfT);
    
    List<T> finalRetList = new LinkedList<T>();
    
    NavigableMap<Key, Object> dataMap = DBHelper.getCreateDataTreeMap(classOfT);
    
    for(Key dataKey : finalKeySet) {
     T value = (T) dataMap.get(dataKey);
      if(value != null)
        finalRetList.add(value);
      else
        log.warning("There is no value for data index: " + dataKey);
    }
    
    return finalRetList;
  }
  
  private <T> Set<Key> getKeysFromQuery(Query query, Class<T> classOfT) {
    
    Set<Key> finalKeyList = new TreeSet<Key>();
    
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
    
    return finalKeyList;
  }
  
  private <T> Set<Key> applyFilter(FieldFilter fieldFilter, Class<T> classOfT) {
    
    log.info("Applying filter: name: '" + fieldFilter.getFieldName() + "' => " + fieldFilter.getFieldValue());
    
    ClassInfo classInfo = DBHelper.getClassInfo(classOfT);
    
    Set<Key> extractKeyList = new TreeSet<Key>();
    
    // we make sure we don't query for fields that are not indexed
    if(!classInfo.indexedFieldNameList.contains(fieldFilter.getFieldName()))
      throw new RuntimeException("The field: '" + fieldFilter.getFieldName() + "' is not indexed and cannot be used in a query");
    
    Key fieldValueKey = Key.fromKeyValue((Comparable) fieldFilter.getFieldValue());
    
    SortedMap<Key,TreeSet<Key>> indexTreeMap = DBHelper.getCreateIndexTreeMap(classOfT, fieldFilter.getFieldName());
    
    log.info("The indexTreeMap has " + indexTreeMap.keySet().size() + " keys");
    /*
    for(Key key : indexTreeMap.keySet()) {
      
      log.info("Key: " + key);
      
      for(Key dataKey : indexTreeMap.get(key))
        log.info("******** " + dataKey);
    }*/
    
    SortedMap<Key,TreeSet<Key>> subsetIndexTreeMap = new TreeMap<Key, TreeSet<Key>>();
   
    log.info("Filter type: " + fieldFilter.getFieldFilterType());
    boolean inclusive = false;
    switch(fieldFilter.getFieldFilterType()) {
      case EQUAL: 
        inclusive = true;
        break;
      case GREATER_THAN:
        subsetIndexTreeMap = indexTreeMap.tailMap(fieldValueKey);
        subsetIndexTreeMap.remove(fieldValueKey);
        break;
      case GREATER_THAN_INC:
        inclusive = true;
        subsetIndexTreeMap = indexTreeMap.tailMap(fieldValueKey);
        break;
      case LOWER_THAN:
        subsetIndexTreeMap = indexTreeMap.headMap(fieldValueKey);
        break;
      case LOWER_THAN_INC:
        inclusive = true;
        subsetIndexTreeMap = indexTreeMap.headMap(fieldValueKey);
        break;
    }
    
    log.info("The subsetIndexTreeMap has " + subsetIndexTreeMap.keySet().size() + " keys");
    
    // flattens the structure
    for(Key key : subsetIndexTreeMap.keySet())
      extractKeyList.addAll(subsetIndexTreeMap.get(key));
    
    // adds the inclusive members if necessary
    if(inclusive && indexTreeMap.containsKey(fieldValueKey))
      extractKeyList.addAll(indexTreeMap.get(fieldValueKey));
    
    log.info("The extracted ket list has " + extractKeyList.size() + " elements");
    
    return extractKeyList;
  }
  
  
  private <T> Set<Key> applyOrder(OrderFilter orderFilter, Set<Key> filteredKeyList, Class<T> classOfT) {
    
    ClassInfo classInfo = DBHelper.getClassInfo(classOfT);
    
    List<Key> extractKeyList = new LinkedList<Key>();
    
    // we make sure we don't order for fields that are NOT indexed
    if(!classInfo.indexedFieldNameList.contains(orderFilter.getFieldName()))
      throw new RuntimeException("The field: '" + orderFilter.getFieldName() + "' is not indexed and cannot be used in a query");
    
    SortedMap<Key,TreeSet<Key>> indexTreeMap = DBHelper.getCreateIndexTreeMap(classOfT, orderFilter.getFieldName());
    
    // we flatten the structure
    for(TreeSet<Key> tempTreeSet : indexTreeMap.values())
      extractKeyList.addAll(tempTreeSet);
    
    extractKeyList.retainAll(filteredKeyList);
    
    //log.info("Extract key list: " + extractKeyList);
        
    if(orderFilter.getOrderType() == OrderType.DESCENDING)
      Collections.reverse(extractKeyList);
    
    return new LinkedHashSet<Key>(extractKeyList);
    
  }
  
}

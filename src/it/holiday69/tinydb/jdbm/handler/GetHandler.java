/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.handler;

import it.holiday69.dataservice.query.FieldFilter;
import it.holiday69.dataservice.query.OrderFilter;
import it.holiday69.dataservice.query.OrderType;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.jdbm.TinyDBHelper;
import it.holiday69.tinydb.jdbm.exception.TinyDBException;
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
    
    return (T) TinyDBHelper.getCreateDataTreeMap(classOfT).get(key);
  }
  
  public <T> List<T> getAll(Class<T> classOfT) {
    
    Collection<T> values = (Collection<T>) TinyDBHelper.getCreateDataTreeMap(classOfT).values();
    if(values != null)
      return new LinkedList<T>(values);
    else
      return new LinkedList<T>();
  }
  
  public <T> T getAny(Class<T> classOfT) {
    
    Map.Entry<Key, Object> firstEntry = TinyDBHelper.getCreateDataTreeMap(classOfT).firstEntry();
    
    if(firstEntry == null)
      return null;
    
    return (T) firstEntry.getValue();
  }
  
  public <T> T getFromQuery(Query query, Class<T> classOfT) {
    
    List<Key> finalKeyList = getKeysFromQuery(query, classOfT);
    
    NavigableMap<Key, Object> dataMap = TinyDBHelper.getCreateDataTreeMap(classOfT);
    
    for(Key dataKey : finalKeyList) {
     T value = (T) dataMap.get(dataKey);
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
    
    NavigableMap<Key, Object> dataMap = TinyDBHelper.getCreateDataTreeMap(classOfT);
    
    for(Key dataKey : finalKeyList) {
     T value = (T) dataMap.get(dataKey);
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
      throw new TinyDBException("Multiple order filters are not supported at the moment, please use just one");
    
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
    
    ClassInfo classInfo = TinyDBHelper.getClassInfo(classOfT);
    
    Set<Key> extractKeyList = new TreeSet<Key>();
    
    // we make sure we don't query for fields that are not indexed
    if(!classInfo.indexedFieldNameList.contains(fieldFilter.getFieldName()))
      throw new TinyDBException("The field: '" + fieldFilter.getFieldName() + "' is not indexed and cannot be used in a query");
    
    Key fieldValueKey = Key.fromKeyValue((Comparable) fieldFilter.getFieldValue());
    
    SortedMap<Key,TreeSet<Key>> indexTreeMap = TinyDBHelper.getCreateIndexTreeMap(classOfT, fieldFilter.getFieldName());
    
    SortedMap<Key,TreeSet<Key>> subsetIndexTreeMap = new TreeMap<Key, TreeSet<Key>>();
   
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
    
    // flattens the structure
    for(Key key : subsetIndexTreeMap.keySet())
      extractKeyList.addAll(subsetIndexTreeMap.get(key));
    
    // adds the inclusive members if necessary
    if(inclusive && indexTreeMap.containsKey(fieldValueKey))
      extractKeyList.addAll(indexTreeMap.get(fieldValueKey));
    
    return extractKeyList;
  }
  
  
  private <T> List<Key> applyOrder(OrderFilter orderFilter, List<Key> filteredKeyList, Class<T> classOfT) {
    
    ClassInfo classInfo = TinyDBHelper.getClassInfo(classOfT);
    
    List<Key> extractKeyList = new LinkedList<Key>();
    
    // we make sure we don't order for fields that are NOT indexed
    if(!classInfo.indexedFieldNameList.contains(orderFilter.getFieldName()))
      throw new TinyDBException("The field: '" + orderFilter.getFieldName() + "' is not indexed and cannot be used in a query");
    
    SortedMap<Key,TreeSet<Key>> indexTreeMap = TinyDBHelper.getCreateIndexTreeMap(classOfT, orderFilter.getFieldName());
    
    // we flatten the index structure
    for(TreeSet<Key> tempTreeSet : indexTreeMap.values())
      extractKeyList.addAll(tempTreeSet);
    
    extractKeyList.retainAll(filteredKeyList);
    
    if(orderFilter.getOrderType() == OrderType.DESCENDING)
      Collections.reverse(extractKeyList);
    
    return extractKeyList;
    
  }
  
  public <T> long getResultSetSize(Class<T> classOfT) {
    return TinyDBHelper.getCreateDataTreeMap(classOfT).keySet().size();
  }

  public <T> long getResultSetSize(Query query, Class<T> classOfT) {
    return getKeysFromQuery(query, classOfT).size();
  }
}

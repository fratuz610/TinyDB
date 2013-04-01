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

import it.holiday69.dataservice.query.FieldFilter;
import it.holiday69.dataservice.query.OrderFilter;
import it.holiday69.dataservice.query.OrderType;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.bitcask.Bitcask;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.db.BitcaskManager;
import it.holiday69.tinydb.db.TinyDBMapper;
import it.holiday69.tinydb.db.vo.ClassInfo;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class GetHandler {
  
  private final Logger _log = Logger.getLogger(GetHandler.class.getSimpleName());
  
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
        _log.warning("GetHandler:getFromQuery: There is no value for data index: " + dataKey);
    }
    return null;
  }
  
  public <T> List<T> getListFromQuery(Query query, Class<T> classOfT) {
    
    _log.fine("Get list from query");
    
    List<Key> finalKeyList = getKeysFromQuery(query, classOfT);
    
    _log.fine("Got " + finalKeyList.size() + " keys");
    
    List<T> finalRetList = new LinkedList<T>();
    
    Bitcask db = _bitcaskManager.getEntityDB(classOfT);
    
    for(Key dataKey : finalKeyList) {
     T value = (T) db.get(dataKey);
      if(value != null)
        finalRetList.add(value);
      else
        _log.warning("GetHandler:getListFromQuery: There is no value for data index: " + dataKey);
    }
    
    return finalRetList;
  }
  
  public <T> List<Key> getKeysFromQuery(Query query, Class<T> classOfT) {
    
    List<Key> finalKeyList = new LinkedList<Key>();
    
    // we apply field filters
    if(!query.getFieldFilterList().isEmpty()) {
      
      // we apply the filters
      for(FieldFilter fieldFilter : query.getFieldFilterList()) {

        if(finalKeyList.isEmpty())
          finalKeyList.addAll(applyFilter(fieldFilter, classOfT));
        else
          finalKeyList.retainAll(applyFilter(fieldFilter, classOfT));
      }
    } else {
      
      _log.fine("Getting all keys for '" + classOfT.getSimpleName() + "'");
      
      // no filters to apply, get all entities
      finalKeyList.addAll(_bitcaskManager.getEntityDB(classOfT).keySet());
    }
    
    _log.fine("Field Filters applied: " + finalKeyList + " elems");
        
    if(query.getOrderFilterList().size() > 1)
      throw new RuntimeException("Multiple order filters are not supported at the moment, please use just one");
    
    if(!query.getOrderFilterList().isEmpty())
      finalKeyList = applyOrder(query.getOrderFilterList().get(0), finalKeyList, classOfT);
    
    _log.fine("Order Filters applied: " + finalKeyList.size()+ " elems");
    
    // we apply the offset if any
    if(query.getOffset() < finalKeyList.size())
      finalKeyList = finalKeyList.subList(query.getOffset(), finalKeyList.size());
    
    _log.fine("Offset applied: " + finalKeyList.size()+ " elems");
    
    // we apply the limit if any
    if(query.getLimit() <= finalKeyList.size())
      finalKeyList = finalKeyList.subList(0, query.getLimit());
    
    _log.fine("Limit applied: " + finalKeyList.size()+ " elems");
    
    return finalKeyList;
  }
  
  private <T> List<Key> applyFilter(FieldFilter fieldFilter, Class<T> classOfT) {

    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    if(fieldFilter.getFieldName().equals(classInfo.idFieldName))
      return applyPrimaryKeyFilter(fieldFilter, classOfT);
    else
      return applyFieldFilter(fieldFilter, classOfT);
  }
  
  private <T> List<Key> applyFieldFilter(FieldFilter fieldFilter, Class<T> classOfT) {
    
    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    // we make sure we don't query for fields that are not indexed
    if(!classInfo.indexedFieldNameList.contains(fieldFilter.getFieldName()))
      throw new RuntimeException("The field: '" + fieldFilter.getFieldName() + "' is not indexed and cannot be used in a query");
    
    List<Key> extractKeyList = new LinkedList<Key>();
      
    Bitcask fieldIndexDB = _bitcaskManager.getIndexDB(classOfT, fieldFilter.getFieldName());
    
    Key fieldValueKey = new Key().fromComparable((Comparable) fieldFilter.getFieldValue());
    
    SortedMap<Key,Object> subsetIndexTreeMap = new TreeMap<Key, Object>();
    
    boolean inclusive = false;
    switch(fieldFilter.getFieldFilterType()) {
      case EQUAL: 
        inclusive = true;
        break;
      case GREATER_THAN:
        subsetIndexTreeMap.putAll(fieldIndexDB.tailMap(fieldValueKey));
        subsetIndexTreeMap.remove(fieldValueKey);
        break;
      case GREATER_THAN_INC:
        inclusive = true;
        subsetIndexTreeMap.putAll(fieldIndexDB.tailMap(fieldValueKey));
        break;
      case LOWER_THAN:
        subsetIndexTreeMap.putAll(fieldIndexDB.headMap(fieldValueKey));
        break;
      case LOWER_THAN_INC:
        inclusive = true;
        subsetIndexTreeMap.putAll(fieldIndexDB.headMap(fieldValueKey));
        break;
    }
    
    // flattens the structure
    for(Key key : subsetIndexTreeMap.keySet())
      extractKeyList.addAll((TreeSet<Key>) subsetIndexTreeMap.get(key));

    // adds the inclusive members if necessary
    if(inclusive && fieldIndexDB.containsKey(fieldValueKey))
      extractKeyList.addAll((TreeSet<Key>) fieldIndexDB.get(fieldValueKey));
      
    return extractKeyList;
  }
  
  private <T> List<Key> applyPrimaryKeyFilter(FieldFilter fieldFilter, Class<T> classOfT) {
    
    List<Key> extractKeyList = new LinkedList<Key>();
    
    Bitcask indexDB = _bitcaskManager.getEntityDB(classOfT);
    
    Key fieldValueKey = new Key().fromComparable((Comparable) fieldFilter.getFieldValue());
    
    boolean inclusive = false;
    switch(fieldFilter.getFieldFilterType()) {
      case EQUAL: 
        inclusive = true;
        break;
      case GREATER_THAN:
        extractKeyList.addAll(indexDB.tailMap(fieldValueKey).keySet());
        break;
      case GREATER_THAN_INC:
        inclusive = true;
        extractKeyList.addAll(indexDB.tailMap(fieldValueKey).keySet());
        break;
      case LOWER_THAN:
        extractKeyList.addAll(indexDB.headMap(fieldValueKey).keySet());
        break;
      case LOWER_THAN_INC:
        inclusive = true;
        extractKeyList.addAll(indexDB.headMap(fieldValueKey).keySet());
        break;
    }
    
    // adds the inclusive members if necessary
    if(inclusive && indexDB.containsKey(fieldValueKey))
      extractKeyList.add(fieldValueKey);
    
    return extractKeyList;
  }
  
  
  private <T> List<Key> applyOrder(OrderFilter orderFilter, List<Key> filteredKeyList, Class<T> classOfT) {
    
    ClassInfo classInfo = _dbMapper.getClassInfo(classOfT);
    
    List<Key> orderedKeyList = new LinkedList<Key>();
    
    // we check if the order is on the primary key
    if(classInfo.idFieldName.equals(orderFilter.getFieldName())) {
      
      _log.finer("Imposing order by primary key: " + classInfo.idFieldName);
      
      // key ordering
      orderedKeyList = filteredKeyList;
      
    } else {
      // we check if the order is on the any field
      // we make sure we don't order for fields that are NOT indexed
      if(!classInfo.indexedFieldNameList.contains(orderFilter.getFieldName()))
        throw new RuntimeException("The field: '" + orderFilter.getFieldName() + "' is not indexed and cannot be used in a query");

      _log.finer("Imposing order by field: " + orderFilter.getFieldName());
      
      Bitcask indexTreeMap = _bitcaskManager.getIndexDB(classOfT, orderFilter.getFieldName());

      _log.finer("The index has cardinality: " + indexTreeMap.size());
      
      // reorganize as array list for fast lookups
      orderedKeyList = new ArrayList<Key>(indexTreeMap.size());
      
      long start = new Date().getTime();
      
      // we flatten the index structure
      for(Object tempTreeSetObj : indexTreeMap.values()) {
        orderedKeyList.addAll((TreeSet<Key>) tempTreeSetObj);
      }
      
      Set<Key> filteredHashKeySet = new HashSet<Key>(filteredKeyList);
      List<Key> tempOrderedList = new ArrayList<Key>(filteredKeyList.size());
      
      for(Key key : orderedKeyList) 
        if(filteredHashKeySet.contains(key))
          tempOrderedList.add(key);
      
      orderedKeyList = tempOrderedList;
      
      long end = new Date().getTime();
      
      _log.info("Applying order time: " + (end-start) + " secs");
      
      _log.finer("After imposing order on "+orderFilter.getFieldName()+" extractKeyList has " + orderedKeyList.size() + " elements");
    }
    
    //Collections.sort(orderedKeyList);
    
    if(orderFilter.getOrderType() == OrderType.DESCENDING)
      Collections.reverse(orderedKeyList);
    
    return orderedKeyList;
  }
  
  public <T> long getResultSetSize(Class<T> classOfT) {
    return _bitcaskManager.getEntityDB(classOfT).keySet().size();
  }

  public <T> long getResultSetSize(Query query, Class<T> classOfT) {
    return getKeysFromQuery(query, classOfT).size();
  }
}

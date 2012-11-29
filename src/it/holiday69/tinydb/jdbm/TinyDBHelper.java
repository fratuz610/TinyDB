/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm;

import it.holiday69.tinydb.jdbm.annotations.Id;
import it.holiday69.tinydb.jdbm.annotations.Indexed;
import it.holiday69.tinydb.jdbm.exception.TinyDBException;
import it.holiday69.tinydb.jdbm.vo.ClassInfo;
import it.holiday69.tinydb.jdbm.vo.Key;
import java.lang.reflect.Field;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class TinyDBHelper {
  
  private final static Logger log = Logger.getLogger(TinyDBHelper.class.getSimpleName());
  
  private static final Map<Class, ClassInfo> _classInfoMap = new HashMap<Class, ClassInfo>();
  
  private static String getIDFieldName(Class<?> clazz)  {
    
    for(Field field : clazz.getDeclaredFields()) {
            
      if(field.isAnnotationPresent(Id.class))
        return field.getName();
    }
    
    throw new TinyDBException("The class: " + clazz.getName() + " has no @Id annotated field");
  }
  
  private static List<String> getIndexedFieldList(Class<?> clazz) {
    List<String> retList = new LinkedList<String>();
    
    for(Field field : clazz.getDeclaredFields()) {
            
      if(field.isAnnotationPresent(Indexed.class)) {
        //log.info(clazz + " Indexed field: " + field.getName());
        retList.add(field.getName());
      }
    }
    
    return retList;
  }
  
  private static void mapClass(Class<?> clazz) {
    
    ClassInfo classInfo = new ClassInfo();
    classInfo.idFieldName = getIDFieldName(clazz);
    
    // determines if the id field is a Long or a String
    Field idField = null;
    try {
      idField = clazz.getField(classInfo.idFieldName);
    } catch(NoSuchFieldException ex) {
      throw new TinyDBException("The class: " + clazz.getName() + " has no @Id annotated field");
    }
          
    if(idField.getType() == String.class) {
      classInfo.idFieldType = ClassInfo.IDFieldType.STRING;
    } else if(idField.getType() == long.class || idField.getType() == Long.class) {
      classInfo.idFieldType = ClassInfo.IDFieldType.LONG;
    } else {
      throw new TinyDBException("Field " + classInfo.idFieldName  + " for class " + clazz.getName() + " is marked as @Id but its type is neither String or Long/long");
    }
    
    // adds the indexed field list
    classInfo.indexedFieldNameList = getIndexedFieldList(clazz);
    
    // updates the shared map
    synchronized(_classInfoMap) {
      _classInfoMap.put(clazz, classInfo); 
    }
    
  }
  
  public static ClassInfo getClassInfo(Class<?> clazz) {
    
    synchronized(_classInfoMap) {
      if(!_classInfoMap.containsKey(clazz))
        mapClass(clazz);

      return _classInfoMap.get(clazz);
    }
  }
  
  // Get / set id field values
  public static Comparable getIDFieldValue(Object obj) {
    ClassInfo classInfo = getClassInfo(obj.getClass());
    return getFieldValue(obj, classInfo.idFieldName);
  }
  
  public static void setIDFieldValue(Object obj, Object fieldValue) {
    
    ClassInfo classInfo = getClassInfo(obj.getClass());
    
    try {
      obj.getClass().getDeclaredField(classInfo.idFieldName).set(obj, fieldValue);
    } catch (Throwable th) {
      throw new TinyDBException("Unable to set id field " + classInfo.idFieldName + " for class " + obj.getClass() + " because: " + th.getMessage());
    }
    
  }
  
  public static Comparable getFieldValue(Object obj, String fieldName) {
    
    try {
      return (Comparable) obj.getClass().getField(fieldName).get(obj);
    } catch(Throwable th) {
      throw new TinyDBException("Unable to access field " + fieldName + " for class " + obj.getClass() + " because: " + th.getMessage());
    }
  }
  
  public static NavigableMap<Key, Object> getCreateDataTreeMap(Class<?> clazz) {
    
    return TinyDB.getInstance().getTreeMap(clazz.getName());
    
  }
  
  public static NavigableMap<Key, TreeSet<Key>> getCreateIndexTreeMap(Class<?> clazz, String indexedFieldName) {
    
    String indexName = "index__" + clazz.getName() + "__" + indexedFieldName;
    
    return TinyDB.getInstance().getTreeMap(indexName);
    
  }
  
  
}
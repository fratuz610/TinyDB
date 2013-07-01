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

package it.holiday69.tinydb.db;

import it.holiday69.tinydb.db.annotations.Id;
import it.holiday69.tinydb.db.annotations.Indexed;
import it.holiday69.tinydb.db.vo.ClassInfo;
import java.lang.reflect.Field;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class TinyDBMapper {
  
  private final static Logger log = Logger.getLogger(TinyDBMapper.class.getSimpleName());
  
  private final Map<Class, ClassInfo> _classInfoMap = new HashMap<Class, ClassInfo>();
  
  /** 
   * Thread Safe
   * 
   */
  public ClassInfo getClassInfo(Class<?> clazz) {

   synchronized(_classInfoMap) {
     if(!_classInfoMap.containsKey(clazz))
       mapClass(clazz);

     return _classInfoMap.get(clazz);
   }
  }
  
  // Get / set id field values
  public Comparable getIDFieldValue(Object obj) {
    ClassInfo classInfo = getClassInfo(obj.getClass());
    
    if(getFieldValue(obj, classInfo.idFieldName).isEmpty())
      return null;
    else
      return getFieldValue(obj, classInfo.idFieldName).iterator().next();
  }
  
  public void setIDFieldValue(Object obj, Object fieldValue) {
    
    ClassInfo classInfo = getClassInfo(obj.getClass());
    
    try {
      obj.getClass().getDeclaredField(classInfo.idFieldName).set(obj, fieldValue);
    } catch (Throwable th) {
      throw new RuntimeException("Unable to set id field " + classInfo.idFieldName + " for class " + obj.getClass() + " because: " + th.getMessage());
    }
    
  }
  
  public static Collection<Comparable> getFieldValue(Object obj, String fieldName) {
    
    if(obj == null)
      throw new NullPointerException("TinyDBMapper:getFieldValue: Null object passed");
    
    try {
      Object fieldValue = obj.getClass().getField(fieldName).get(obj);
      
      if(fieldValue == null)
        return new ArrayList<Comparable>();
      
      if(fieldValue instanceof Collection)
        return (Collection<Comparable>) fieldValue; 
      else if(fieldValue instanceof Comparable) {
        List<Comparable> retList = new ArrayList<Comparable>();
        retList.add((Comparable) fieldValue);
        return retList;
      } else
        throw new RuntimeException("Unindexable field '" + fieldName + "' because the field value is not a Collection or a Comparable object");

    } catch(Throwable th) {
      if(obj != null)
        throw new RuntimeException("Unable to access field " + fieldName + " for class " + obj.getClass() + " because: " + th.getMessage());
      else
        throw new RuntimeException("Unable to access field " + fieldName + " for class " + obj + " because: " + th.getMessage());
    }
  }
  
  
  private String getIDFieldName(Class<?> clazz)  {
    
    for(Field field : clazz.getDeclaredFields()) {
            
      if(field.isAnnotationPresent(Id.class))
        return field.getName();
    }
    
    throw new RuntimeException("The class: " + clazz.getName() + " has no @Id annotated field");
  }
  
  private List<String> getIndexedFieldList(Class<?> clazz) {
    List<String> retList = new LinkedList<String>();
    
    for(Field field : clazz.getDeclaredFields()) {
            
      if(field.isAnnotationPresent(Indexed.class)) {
        //log.info(clazz + " Indexed field: " + field.getName());
        retList.add(field.getName());
      }
    }
    
    return retList;
  }
  
  private void mapClass(Class<?> clazz) {
    
    ClassInfo classInfo = new ClassInfo();
    classInfo.idFieldName = getIDFieldName(clazz);
    
    // determines if the id field is a Long or a String
    Field idField = null;
    try {
      idField = clazz.getField(classInfo.idFieldName);
    } catch(NoSuchFieldException ex) {
      throw new RuntimeException("The class: " + clazz.getName() + " has no @Id annotated field");
    }
          
    if(idField.getType() == String.class) {
      classInfo.idFieldType = ClassInfo.IDFieldType.STRING;
    } else if(idField.getType() == long.class || idField.getType() == Long.class) {
      classInfo.idFieldType = ClassInfo.IDFieldType.LONG;
    } else if(idField.getType() == double.class || idField.getType() == Double.class) {
      classInfo.idFieldType = ClassInfo.IDFieldType.DOUBLE;
    } else {
      throw new RuntimeException("Field " + classInfo.idFieldName  + " for class " + clazz.getName() + " is marked as @Id but its type is neither String or Long/long");
    }
    
    // adds the indexed field list
    classInfo.indexedFieldNameList = getIndexedFieldList(clazz);
    
    // updates the shared map
    synchronized(_classInfoMap) {
      _classInfoMap.put(clazz, classInfo); 
    }
    
  }
  
 
    
}

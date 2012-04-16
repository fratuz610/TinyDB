/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.query;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author fratuz610
 */
public class Query  {
  
  private final List<FieldFilter> _fieldFilterList = new LinkedList<FieldFilter>();
  private final List<OrderFilter> _orderFilterList = new LinkedList<OrderFilter>();
  
  public Query() { }
  
  public Query filter(String fieldName, Object fieldValue, FieldFilterType fieldFilterType) { 
    
    fieldName = fieldName.trim();
    
    _fieldFilterList.add(new FieldFilter(fieldName, fieldValue, fieldFilterType));
    return this; 
  }
  
  public Query filter(String fieldName, Object fieldValue) { 
    
    fieldName = fieldName.trim();
    
    String parsedFieldName;
    
    if(fieldName.indexOf(" ") != -1)
      parsedFieldName = fieldName.substring(0, fieldName.lastIndexOf(" "));
    else
      parsedFieldName = fieldName;
    
    if(fieldName.endsWith(" =") ||  fieldName.endsWith(" =="))
      _fieldFilterList.add(new FieldFilter(parsedFieldName, fieldValue, FieldFilterType.EQUAL));
    else if(fieldName.endsWith(" >"))
      _fieldFilterList.add(new FieldFilter(parsedFieldName, fieldValue, FieldFilterType.GREATER_THAN));
    else if(fieldName.endsWith(" >="))
      _fieldFilterList.add(new FieldFilter(parsedFieldName, fieldValue, FieldFilterType.GREATER_THAN_INC));
    else if(fieldName.endsWith(" <"))
      _fieldFilterList.add(new FieldFilter(parsedFieldName, fieldValue, FieldFilterType.LOWER_THAN));
    else if(fieldName.endsWith(" <="))
      _fieldFilterList.add(new FieldFilter(parsedFieldName, fieldValue, FieldFilterType.LOWER_THAN_INC));
    else
      _fieldFilterList.add(new FieldFilter(fieldName, fieldValue, FieldFilterType.EQUAL));
    
    return this; 
  }
  
  public Query orderBy(String fieldName, OrderType orderType) {
    
    fieldName = fieldName.trim();
    
    _orderFilterList.add(new OrderFilter(fieldName, orderType));
    return this;
  }
  
  public Query orderBy(String fieldName) {
    
    fieldName = fieldName.trim();
    
    _orderFilterList.add(new OrderFilter(fieldName, OrderType.ASCENDING));
    
    if(!_orderFilterList.isEmpty())
      throw new RuntimeException("Multiple sortBy fields are not allowed yet, please ordery by one field only");
    
    return this;
  }
  
  public List<FieldFilter> getFieldFilterList() { return _fieldFilterList; }
  public List<OrderFilter> getOrderFilterList() { return _orderFilterList; }
  
}

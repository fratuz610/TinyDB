/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.query;

/**
 *
 * @author fratuz610
 */
public class FieldFilter {
    
    private String _fieldName;
    private Object _fieldValue;
    private FieldFilterType _fieldFilterType;
    
    public FieldFilter(String fieldName, Object fieldValue, FieldFilterType fieldFilterType) {
      _fieldName = fieldName;
      _fieldValue = fieldValue;
      _fieldFilterType = fieldFilterType;
    }
    
    public String getFieldName() { return _fieldName; }
    public Object getFieldValue() { return _fieldValue; }
    public FieldFilterType getFieldFilterType() { return _fieldFilterType; }
  }

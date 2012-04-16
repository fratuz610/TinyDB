/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.query;

/**
 *
 * @author fratuz610
 */
public class OrderFilter {
    
    private String _fieldName;
    private OrderType _orderType;
    
    public OrderFilter(String fieldName, OrderType orderType) {
      _fieldName = fieldName;
      _orderType = orderType;
    }
    
    public String getFieldName() { return _fieldName; }
    public OrderType getOrderType() { return _orderType; }
  }

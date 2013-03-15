/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.vo;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author fratuz610
 */
public class ClassInfo {
  
  public enum IDFieldType { STRING, LONG }
  
  public String idFieldName;
  public IDFieldType idFieldType;
  public List<String> indexedFieldNameList = new LinkedList<String>();
}

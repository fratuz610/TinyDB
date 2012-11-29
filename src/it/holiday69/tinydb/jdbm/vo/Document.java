/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.vo;

import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author fratuz610
 */
public class Document extends HashMap<String, Serializable> {

  private final static String ID_KEY = "__id__";
  
  public void setID(Serializable idValue) { put(ID_KEY, idValue); }
  
  public Serializable getID() { return get(ID_KEY); }
  
}

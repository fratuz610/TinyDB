/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.vo;

import it.holiday69.tinydb.jdbm.TinyDBDataService;
import java.io.Serializable;

/**
 * A wrapper for any {@link Comparable} object used as a key in the {@link TinyDBDataService}
 * </p>
 * The keyValue must implement {@link Serializable} as well
 * @author fratuz610
 */
public class Key implements Comparable<Key>, Serializable {
  
  public Comparable keyValue = 0l;
  
  private Key() { }
  
  public static Key fromKeyValue(Comparable keyValue) { 
    Key newKey = new Key();
    newKey.keyValue = keyValue;
    return newKey;
  } 

  @Override
  public int compareTo(Key t) {
    return keyValue.compareTo(t.keyValue);
  }
  
  @Override
  public boolean equals(Object obj) {
    
    if(!(obj instanceof Key))
      return false;
    
    return keyValue.equals(((Key) obj).keyValue);
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 59 * hash + (this.keyValue != null ? this.keyValue.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    return "[Key " + keyValue.getClass() + " / " + keyValue.toString() + " ]";
  }
}

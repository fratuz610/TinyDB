/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.entity;

/**
 *
 * @author fratuz610
 */
public class RecordRef {
  
  public int fileRef;
  public int offset;
  public int size;
  
  public String toString() { return "[RecordRef] fileRef: " + fileRef + " offset: " + offset + " size: " + size + " [/RecordRef]"; } 
}

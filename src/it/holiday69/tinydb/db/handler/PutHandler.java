/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.handler;

/**
 *
 * @author Stefano
 */
public interface PutHandler<T> {
  
  public <T> void put(T newObj);
  
  public void shutdown();
  
}

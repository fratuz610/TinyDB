/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm.exception;

import it.holiday69.tinydb.jdbm.TinyDBDataService;

/**
 * An exception thrown if any configuration error concerning the {@link TinyDBDataService} arises
 * @author fratuz610
 */
public class TinyDBException extends RuntimeException {
  
  public TinyDBException(String message) {
    super(message);
  }
  
}

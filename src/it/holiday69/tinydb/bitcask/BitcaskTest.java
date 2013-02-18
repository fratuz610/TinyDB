/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask;

import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class BitcaskTest {
  
  private final Logger _log = Logger.getLogger(BitcaskTest.class.getSimpleName());
  
  public static void main(String[] args) {
    
    Bitcask bitcask = new Bitcask(new BitcaskOptions().withDbName("hello-world"));
    
    bitcask.addRecord("key", "value supervalue".getBytes());
    
    byte[] retArray = bitcask.getRecord("key");
    
    String ret = new String(retArray);
    
    System.out.println("ret: " + ret + "  original: " + "value supervalue");
            
            
    
    
  }
}

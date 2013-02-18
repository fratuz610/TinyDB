/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask;

import java.io.File;

/**
 *
 * @author Stefano
 */
public class BitcaskOptions {
  
  public String dbName = "bitcask";
  public String dbFolder = new File(".").getAbsolutePath();
  public int recordPerFile = 100;
  
  public BitcaskOptions withDbName(String dbName) { this.dbName = dbName; return this; }
  public BitcaskOptions withDbFolder(String dbFolder) { this.dbFolder = dbFolder; return this; }
  public BitcaskOptions withRecordPerFile(int recordPerFile) { this.recordPerFile = recordPerFile; return this; }
  
}

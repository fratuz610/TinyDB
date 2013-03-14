/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Stefano
 */
public class BitcaskOptions {
  
  public String dbName = null;
  public String dbFolder = new File(".", "data").getAbsolutePath();
  public int recordPerFile = 100;
  public int compactFrequency = 5;
  public TimeUnit compactTimeUnit = TimeUnit.MINUTES;
  
  public BitcaskOptions withDbName(String dbName) { this.dbName = dbName; return this; }
  public BitcaskOptions withDbFolder(String dbFolder) { this.dbFolder = dbFolder; return this; }
  public BitcaskOptions withRecordPerFile(int recordPerFile) { this.recordPerFile = recordPerFile; return this; }
  public BitcaskOptions withCompactEvery(int frequency, TimeUnit timeUnit) { this.compactFrequency = frequency; this.compactTimeUnit = timeUnit; return this; }
  
}

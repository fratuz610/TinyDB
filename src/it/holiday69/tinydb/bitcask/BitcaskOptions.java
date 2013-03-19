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
  
  public String dbFolder = new File(".", "data").getAbsolutePath();
  public int recordPerFile = 100;
  public int compactFrequency = 5;
  public TimeUnit compactTimeUnit = TimeUnit.MINUTES;
  public int cacheSize = 1024 * 1024; // 512 kb cache
  public boolean autoCompact = true;
  
  public BitcaskOptions withDbFolder(String dbFolder) { this.dbFolder = dbFolder; return this; }
  public BitcaskOptions withRecordPerFile(int recordPerFile) { 
    if(recordPerFile < 10) recordPerFile = 10;
    this.recordPerFile = recordPerFile; 
    return this; 
  }
  public BitcaskOptions withCompactEvery(int frequency, TimeUnit timeUnit) { 
    this.compactFrequency = frequency; 
    this.compactTimeUnit = timeUnit; 
    
    if(TimeUnit.SECONDS.convert(frequency, timeUnit) < 30) {
      this.compactFrequency = 30;
      this.compactTimeUnit = TimeUnit.SECONDS;
    }
    
    return this; 
  }
  public BitcaskOptions withCacheSize(int cacheSize) { this.cacheSize = cacheSize; return this; }
  public BitcaskOptions withAutoCompactEnabled(boolean autoCompact) { this.autoCompact = autoCompact; return this; }
}

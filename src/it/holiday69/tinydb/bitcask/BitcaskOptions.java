/*
    Copyright 2013 Stefano Fratini (mail@stefanofratini.it)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
  public long maxFileSize = 16*1024*1024;
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
  
  public BitcaskOptions withMaxFileSize(long maxFileSize) { 
    if(maxFileSize < 1024*1024) maxFileSize = 1024*1024; // min 1mb
    this.maxFileSize = maxFileSize; 
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

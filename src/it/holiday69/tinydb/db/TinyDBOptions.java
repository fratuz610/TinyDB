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

package it.holiday69.tinydb.db;

import it.holiday69.tinydb.bitcask.BitcaskOptions;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Stefano
 */
public class TinyDBOptions {
   
  public BitcaskOptions bitcaskOptions = new BitcaskOptions();
  
  public TinyDBOptions withDbFolder(String dbFolder) { this.bitcaskOptions.dbFolder = dbFolder; return this; }
  public TinyDBOptions withRecordPerFile(int recordPerFile) { 
    if(recordPerFile < 10) recordPerFile = 10;
    this.bitcaskOptions.recordPerFile = recordPerFile; 
    return this; 
  }
  public TinyDBOptions withCompactEvery(int frequency, TimeUnit timeUnit) { 
    this.bitcaskOptions.compactFrequency = frequency; 
    this.bitcaskOptions.compactTimeUnit = timeUnit; 
    
    if(TimeUnit.SECONDS.convert(frequency, timeUnit) < 30) {
      this.bitcaskOptions.compactFrequency = 30;
      this.bitcaskOptions.compactTimeUnit = TimeUnit.SECONDS;
    }
    return this;
  }
  
  public TinyDBOptions withAutoCompactEnabled(boolean autoCompact) { this.bitcaskOptions.autoCompact = autoCompact; return this; }
  public TinyDBOptions withCacheSize(int cacheSize) { this.bitcaskOptions.cacheSize = cacheSize; return this; }
  
  // tinydb related options
  public boolean asyncUpdates = true;
  public int executorPoolSize = 5;
  
  public TinyDBOptions withAsyncUpdates(boolean asyncUpdates) { this.asyncUpdates = asyncUpdates; return this; }
  public TinyDBOptions withExecutoPoolSize(int executorPoolSize) { this.executorPoolSize = executorPoolSize; return this; }
  
}

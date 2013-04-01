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

package it.holiday69.tinydb.bitcask.vo;

import java.io.File;

/**
 *
 * @author Stefano
 */
public class AppendInfo {
  
    public File appendFile;
    public int keySize;
    public int valueSize;
    public long valuePosition;
    public long timestamp;

    @Override
    public String toString() {
      return "AppendInfo{" + "appendFile=" + appendFile + ", keySize=" + keySize + ", valueSize=" + valueSize + ", valuePosition=" + valuePosition + ", timestamp=" + timestamp + '}';
    }
    
  }

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

package it.holiday69.tinydb.db.utils;

import java.nio.ByteBuffer;

/**
 *
 * @author Stefano Fratini <stefano.fratini@yeahpoint.com>
 */
public class SerialUtils {
  
  public static byte[] longToByteArray(long value) {
    return longToByteArray(value, 8);
  }
  
  public static byte[] longToByteArray(long value, int storageSize) {
    
    if(storageSize > 8 || storageSize < 1)
      throw new IllegalArgumentException("The storage size value should be 1<->8");
    
    ByteBuffer dbuf = ByteBuffer.allocate(8);
    dbuf.putLong(value);
    return ByteArrayUtils.trimLeft(dbuf.array(), 8-storageSize);
  }
  
  public static byte[] intToByteArray(int value) {
    return intToByteArray(value, 4);
  }
  
  public static byte[] intToByteArray(int value, int storageSize) {
    
    if(storageSize > 4 || storageSize < 1)
      throw new IllegalArgumentException("The storage size value should be 1<->8");
    
    ByteBuffer dbuf = ByteBuffer.allocate(4);
    dbuf.putInt(value);
    return ByteArrayUtils.trimLeft(dbuf.array(), 4-storageSize);
  }
  
  public static int byteArrayToInt(byte[] src) {
    
    if(src.length < 4)
      src = ByteArrayUtils.padLeft(src, 4 - src.length);
    
    ByteBuffer wrapped = ByteBuffer.wrap(src); // big-endian by default
    return wrapped.getInt();
  }
  
  public static long byteArrayToLong(byte[] src) {
    
    if(src.length < 8)
      src = ByteArrayUtils.padLeft(src, 8 - src.length);
    
    ByteBuffer wrapped = ByteBuffer.wrap(src); // big-endian by default
    return wrapped.getLong();
  }
  
}

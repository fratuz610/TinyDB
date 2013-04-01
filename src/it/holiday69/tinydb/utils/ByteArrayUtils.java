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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author Stefano Fratini <stefano.fratini@yeahpoint.com>
 */
public class ByteArrayUtils {
  
  /**
   * Returns a byte array left padded of padSize zero bytes
   * @param src
   * @param padSize
   * @return 
   */
  public static byte[] padLeft(byte[] src, int padSize) {
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      
      while(padSize > 0) {
        padSize--;
        bout.write(0);
      }
      bout.write(src);
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    
    return bout.toByteArray();
  }
  
  /**
   * Returns a byte array left trimmed of trimSize elements
   * @param src
   * @param trimSize
   * @return 
   */
  public static byte[] trimLeft(byte[] src, int trimSize) {
    
    if(trimSize >= src.length)
      throw new IllegalArgumentException("Unable to trim " + trimSize + " elements from an array of " + src.length + " elements " );
    
    return Arrays.copyOfRange(src, trimSize, src.length);
  }
  

}

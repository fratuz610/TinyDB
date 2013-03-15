/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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

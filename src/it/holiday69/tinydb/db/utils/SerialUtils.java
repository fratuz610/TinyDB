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
    ByteBuffer dbuf = ByteBuffer.allocate(8);
    dbuf.putLong(value);
    return dbuf.array();
  }
  
  public static byte[] intToByteArray(int value) {
    ByteBuffer dbuf = ByteBuffer.allocate(4);
    dbuf.putInt(value);
    return dbuf.array();
  }
  
  public static int byteArrayToInt(byte[] src) {
    if(src.length < 4)
      throw new RuntimeException("Unable convert a byte array into Integer: min length is 4 bytes");
    
    ByteBuffer wrapped = ByteBuffer.wrap(src); // big-endian by default
    return wrapped.getInt();
  }
  
  public static long byteArrayToLong(byte[] src) {
    if(src.length < 8)
      throw new RuntimeException("Unable convert a byte array into Long: min length is 8 bytes");
    
    ByteBuffer wrapped = ByteBuffer.wrap(src); // big-endian by default
    return wrapped.getLong();
  }


}

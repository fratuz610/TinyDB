/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.keydir.vo;

import it.holiday69.tinydb.db.utils.SerialUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.zip.CRC32;

/**
 *
 * @author Stefano
 */
public class Record {
  
  public long ts;
  public long keySize;
  public long valueSize;
  public byte[] key;
  public byte[] value;
  
  public Record(byte[] key, byte[] value) {
    
    this.key = key;
    this.value = value;
    keySize = key.length;
    valueSize = value.length;
    ts = new Date().getTime();
  }
  
  public byte[] toByteArray() {
    
    byte[] crcData = null;
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      bout.write(SerialUtils.longToByteArray(ts));
      bout.write(SerialUtils.longToByteArray(keySize));
      bout.write(SerialUtils.longToByteArray(valueSize));
      bout.write(key);
      bout.write(value);
      crcData = bout.toByteArray();
      
      CRC32 crc32 = new CRC32();
      crc32.update(crcData);
      
      bout.reset();
      bout.write(SerialUtils.longToByteArray(crc32.getValue()));
      bout.write(crcData);
      return bout.toByteArray();
      
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  public long relativeValuePosition() {
    return 24 + keySize;
  }

  @Override
  public String toString() {
    return "Record{" + "ts=" + ts + ", keySize=" + keySize + ", valueSize=" + valueSize + ", key=" + key + ", value=" + value + '}';
  }
  
  
  
  
  
}

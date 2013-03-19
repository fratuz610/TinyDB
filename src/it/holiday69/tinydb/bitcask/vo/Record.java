/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.vo;

import it.holiday69.tinydb.bitcask.file.utils.KryoUtils;
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
  public int keySize;
  public int valueSize;
  public byte[] key;
  public byte[] value;
  
  private byte[] crc32Ba;
  
  private byte[] _byteArrayValue;
  private int _valueInternalPos;
  
  public Record(byte[] key, byte[] value) {
    
    this.key = key;
    this.value = value;
    this.keySize = key.length;
    this.valueSize = value.length;
    this.ts = new Date().getTime();
    
    byte[] tsBa = KryoUtils.writeLong(ts);
    byte[] keySizeBa = KryoUtils.writeInt(keySize);
    byte[] valueSizeBa = KryoUtils.writeInt(valueSize);
    
    byte[] crcData = null;
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      bout.write(tsBa);
      bout.write(keySizeBa);
      bout.write(valueSizeBa);
      bout.write(key);
      bout.write(value);
      crcData = bout.toByteArray();
      
      CRC32 crc32 = new CRC32();
      crc32.update(crcData);
      
      crc32Ba = KryoUtils.writeLong(crc32.getValue());
      
      bout.reset();
      bout.write(crc32Ba);
      bout.write(crcData);
      _byteArrayValue = bout.toByteArray();
      
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    
    _valueInternalPos = crc32Ba.length + tsBa.length + keySizeBa.length + valueSizeBa.length + keySize;
    
  }
  
  public byte[] toByteArray() { return _byteArrayValue; }
  
  public long relativeValuePosition() {
    return _valueInternalPos;
  }

  @Override
  public String toString() {
    return "Record{" + "ts=" + ts + ", keySize=" + keySize + ", valueSize=" + valueSize + ", key=" + key + ", value=" + value + '}';
  }
  
  
  
  
  
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.keydir.vo;

import it.holiday69.tinydb.db.utils.SerialUtils;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 *
 * @author Stefano
 */
public class Key implements Comparable<Key> {
  
  public enum Type {LONG, STRING};
  
  public static final int STRING_ENCODING = 0x00;
  public static final int LONG_ENCODING = 0x01;
  
  private Type _type;
  
  private Object _value;
  private byte[] _byteArray;
  
  public Key fromString(String src) {
    
    _type = Type.STRING;
    _value = src;
    
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      bout.write(STRING_ENCODING);
      bout.write(src.getBytes());
      _byteArray = bout.toByteArray();
    } catch(Throwable th) {
      throw new RuntimeException("Unable to convert key to byte arrat: " + ExceptionUtils.getDisplableExceptionInfo(th));
    }
    
    return this;
  }
  
  public Key fromLong(long src) {
    _type = Type.LONG;
    _value = src;
    
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      bout.write(LONG_ENCODING);
      bout.write(SerialUtils.longToByteArray(src));
      _byteArray = bout.toByteArray();
    } catch(Throwable th) {
      throw new RuntimeException("Unable to convert key to byte arrat: " + ExceptionUtils.getDisplableExceptionInfo(th));
    }
    
    return this;
  }
  
  public Key fromByteArray(byte[] src) {
   
    byte keyType = src[0];
    byte[] contentPart = Arrays.copyOfRange(src, 1, src.length);
    
    
    if(keyType == LONG_ENCODING) {
      _type = Type.LONG;
      _value = SerialUtils.byteArrayToLong(contentPart);
    } else if(keyType == STRING_ENCODING) {
      _type = Type.STRING;
      _value = new String(contentPart);
    } else
      throw new IllegalArgumentException("Byte array does not contain any known encoding");
    
    _byteArray = src;
    
    return this;
  }
  
  public byte[] toByteArray() { 
    return _byteArray;
  }
  
  public String toByteArrayString() {
    
    String ret = "";
    for(int i = 0; i < _byteArray.length; i++) {
      ret += "[" + i + " => " + (_byteArray[i] & 0xFF) +" / '"+ (char) _byteArray[i] +"' ] ";
    }
    return ret;
  }
  
  public Type getType() { return _type; }
  
  public Object getValue() { return _value; }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + (this._type != null ? this._type.hashCode() : 0);
    hash = 29 * hash + (this._value != null ? this._value.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Key other = (Key) obj;
    if (this._type != other._type) {
      return false;
    }
    if (this._value != other._value && (this._value == null || !this._value.equals(other._value))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "Key{" + "_type=" + _type + ", _value=" + _value + '}';
  }

  @Override
  public int compareTo(Key o) {
    
    if(_type != o.getType())
      throw new RuntimeException("Unable to compare keys with different types");
    
    switch(_type) {
      case LONG: 
        return ((Long) _value).compareTo((Long) o.getValue()); 
      case STRING: 
        return ((String) _value).compareTo((String) o.getValue());
    }
    
    throw new RuntimeException("Unable to compare keys with unknown types: " + _type);
    
  }
  
}

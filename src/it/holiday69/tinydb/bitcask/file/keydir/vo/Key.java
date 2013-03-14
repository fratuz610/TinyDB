/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.keydir.vo;

import it.holiday69.tinydb.bitcask.file.utils.HessianUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author Stefano
 */
public class Key implements Comparable<Key> {
  
  private Object _keyValue;
  private byte[] _byteArray;
  
  public Key fromString(String src) {
    
    _keyValue = src;
    _byteArray = HessianUtils.writeString(src);
    
    return this;
  }
  
  public Key fromLong(long src) {
    _keyValue = src;
    _byteArray = HessianUtils.writeLong(src);
    return this;
  }
  
  public Key fromDouble(double src) {
    _keyValue = src;
    _byteArray = HessianUtils.writeDouble(src);
    return this;
  }
  
  public Key fromInt(int src) {
    _keyValue = src;
    _byteArray = HessianUtils.writeInt(src);
    return this;
  }
  
  public Key fromByteArray(byte[] src) throws IOException {
   
    _byteArray = src;
    _keyValue = HessianUtils.readObject(new ByteArrayInputStream(src));
    
    if(!(_keyValue instanceof Comparable))
      throw new RuntimeException("Unable to create a key from a non comparable type: " + _keyValue.getClass());
              
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
  
  public Comparable getValue() { return (Comparable) _keyValue; }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 53 * hash + (this._keyValue != null ? this._keyValue.hashCode() : 0);
    hash = 53 * hash + Arrays.hashCode(this._byteArray);
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
    if (this._keyValue != other._keyValue && (this._keyValue == null || !this._keyValue.equals(other._keyValue))) {
      return false;
    }
    if (!Arrays.equals(this._byteArray, other._byteArray)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "Key{" + "_keyValue=" + _keyValue + ", _byteArray=" + _byteArray + '}';
  }

  @Override
  public int compareTo(Key o) {
    return getValue().compareTo(o.getValue());
  }
  
}

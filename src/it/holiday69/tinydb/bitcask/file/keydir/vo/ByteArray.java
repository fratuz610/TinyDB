/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.keydir.vo;

/**
 *
 * @author Stefano
 */
public final class ByteArray
{

  private byte[] array;
  private int    offset;
  private int    length;

  /**
    Create an instance of this class that wraps ths given array.
    This class does not make a copy of the array, it just saves
    the reference.
  */
  public ByteArray(byte[] array, int offset, int length) {
    this.array = array;
    this.offset = offset;
    this.length = length;
  }

  public ByteArray(byte[] array) {
    this(array, 0, array.length);
  }


  /**
    Value equality for byte arrays.
  */
  @Override
  public boolean equals(Object other) {
    if (other instanceof ByteArray) {
      ByteArray ob = (ByteArray) other;
      return ByteArray.equals(array, offset, length, ob.array, ob.offset, ob.length);
    }
    return false;
  }

  /**
  */
  @Override
  public int hashCode() {

    byte[] larray = array;

    int hash = length;
    for (int i = 0; i < length; i++) {
      hash += larray[i + offset];
    }
    return hash;
  }

  public final byte[] getArray() {
    return array;
  }
  public final int getOffset() {
    return offset;
  }

  public final int getLength() {
    return length;
  }
  
  /**
    Compare two byte arrays using value equality.
    Two byte arrays are equal if their length is
    identical and their contents are identical.
  */
  private static boolean equals(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength) {

    if (aLength != bLength)
      return false;

    for (int i = 0; i < aLength; i++) {
      if (a[i + aOffset] != b[i + bOffset])
        return false;
    }
    return true;
  }
}

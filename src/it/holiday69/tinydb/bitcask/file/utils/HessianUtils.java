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

package it.holiday69.tinydb.bitcask.file.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;


/**
 *
 * @author Stefano Fratini <mail@stefanofratini.it>
 */
public class HessianUtils {
  

  private static final int BC_BINARY = 'B'; // final chunk
  private static final int BC_BINARY_CHUNK = 'A'; // non-final chunk
  private static final int BC_BINARY_DIRECT = 0x20; // 1-byte length binary
  private static final int BINARY_DIRECT_MAX = 0x0f;
  private static final int BC_BINARY_SHORT = 0x34; // 2-byte length binary
  private static final int BINARY_SHORT_MAX = 0x3ff; // 0-1023 binary
  
  private static final int INT_DIRECT_MIN = -0x10;
  private static final int INT_DIRECT_MAX = 0x2f;
  private static final int BC_INT_ZERO = 0x90;
  private static final int INT_BYTE_MIN = -0x800;
  private static final int INT_BYTE_MAX = 0x7ff;
  private static final int BC_INT_BYTE_ZERO = 0xc8;
  private static final int INT_SHORT_MIN = -0x40000;
  private static final int INT_SHORT_MAX = 0x3ffff;
  private static final int BC_INT_SHORT_ZERO = 0xd4;
  
  private static final long LONG_DIRECT_MIN = -0x08;
  private static final long LONG_DIRECT_MAX =  0x0f;
  private static final int BC_LONG_ZERO = 0xe0;

  private static final long LONG_BYTE_MIN = -0x800;
  private static final long LONG_BYTE_MAX =  0x7ff;
  private static final int BC_LONG_BYTE_ZERO = 0xf8;

  private static final int LONG_SHORT_MIN = -0x40000;
  private static final int LONG_SHORT_MAX = 0x3ffff;
  private static final int BC_LONG_SHORT_ZERO = 0x3c;
  
  private static final int BC_LONG_INT = 0x59;
  
  private static final int BC_DOUBLE_ZERO = 0x5b;
  private static final int BC_DOUBLE_ONE = 0x5c;
  private static final int BC_DOUBLE_BYTE = 0x5d;
  private static final int BC_DOUBLE_SHORT = 0x5e;
  private static final int BC_DOUBLE_MILL = 0x5f;
  
  private static final int BC_DATE = 0x4a; // 64-bit millisecond UTC date
  private static final int BC_DATE_MINUTE = 0x4b; // 32-bit minute UTC date
  
  private static final int BC_STRING = 'S'; // final string
  private static final int BC_STRING_CHUNK = 'R'; // non-final string
  
  private static final int BC_STRING_DIRECT = 0x00;
  private static final int STRING_DIRECT_MAX = 0x1f;
  private static final int BC_STRING_SHORT = 0x30;
  private static final int STRING_SHORT_MAX = 0x3ff;
  
  private final static int SIZE = 8 * 1024; //8kb
  
  /**
   * Writes a boolean value to the stream.  The boolean will be written
   * with the following syntax:
   *
   * <code><pre>
   * T
   * F
   * </pre></code>
   *
   * @param value the boolean value to write.
   */
  
  public static byte[] writeBoolean(boolean value) {
    
    byte[] ret = new byte[1];
    
    if (value)
      ret[0] = (byte) 'T';
    else
      ret[0] = (byte) 'F';
    
    return ret;
  }
  
  /**
   * Writes a null value to the stream.
   * The null will be written with the following syntax
   *
   * <code><pre>
   * N
   * </pre></code>
   *
   * @param value the string value to write.
   */
  public static byte[] writeNull() {
    byte[] ret = new byte[1];
    ret[0] = (byte) 'N';
    return ret;
  }
  
  
  /**
   * Writes an integer value to the stream.  The integer will be written
   * with the following syntax:
   *
   * <code><pre>
   * I b32 b24 b16 b8
   * </pre></code>
   *
   * @param value the integer value to write.
   */
  
  public static byte[] writeInt(int value){
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();

    if (INT_DIRECT_MIN <= value && value <= INT_DIRECT_MAX) {
      bout.write((byte) (value + BC_INT_ZERO));
    } else if (INT_BYTE_MIN <= value && value <= INT_BYTE_MAX) {
      bout.write((byte) (BC_INT_BYTE_ZERO + (value >> 8)));
      bout.write((byte) (value));
    }
    else if (INT_SHORT_MIN <= value && value <= INT_SHORT_MAX) {
      bout.write((byte) (BC_INT_SHORT_ZERO + (value >> 16)));
      bout.write((byte) (value >> 8));
      bout.write((byte) (value));
    }
    else {
      bout.write((byte) ('I'));
      bout.write((byte) (value >> 24));
      bout.write((byte) (value >> 16));
      bout.write((byte) (value >> 8));
      bout.write((byte) (value));
    }
    
    return bout.toByteArray();
  }
  
  
   /**
   * Writes a long value to the stream.  The long will be written
   * with the following syntax:
   *
   * <code><pre>
   * L b64 b56 b48 b40 b32 b24 b16 b8
   * </pre></code>
   *
   * @param value the long value to write.
   */
  public static byte[] writeLong(long value)
  {
   
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    
    if (LONG_DIRECT_MIN <= value && value <= LONG_DIRECT_MAX) {
      bout.write((byte) (value + BC_LONG_ZERO));
    }
    else if (LONG_BYTE_MIN <= value && value <= LONG_BYTE_MAX) {
      bout.write((byte) (BC_LONG_BYTE_ZERO + (value >> 8)));
      bout.write((byte) (value));
    }
    else if (LONG_SHORT_MIN <= value && value <= LONG_SHORT_MAX) {
      bout.write((byte) (BC_LONG_SHORT_ZERO + (value >> 16)));
      bout.write((byte) (value >> 8));
      bout.write((byte) (value));
    }
    else if (-0x80000000L <= value && value <= 0x7fffffffL) {
      bout.write((byte) BC_LONG_INT);
      bout.write((byte) (value >> 24));
      bout.write((byte) (value >> 16));
      bout.write((byte) (value >> 8));
      bout.write((byte) (value));
    }
    else {
      bout.write((byte) 'L');
      bout.write((byte) (value >> 56));
      bout.write((byte) (value >> 48));
      bout.write((byte) (value >> 40));
      bout.write((byte) (value >> 32));
      bout.write((byte) (value >> 24));
      bout.write((byte) (value >> 16));
      bout.write((byte) (value >> 8));
      bout.write((byte) (value));
    }

    return bout.toByteArray();
  }
  
  /**
   * Writes a double value to the stream.  The double will be written
   * with the following syntax:
   *
   * <code><pre>
   * D b64 b56 b48 b40 b32 b24 b16 b8
   * </pre></code>
   *
   * @param value the double value to write.
   */
  public static byte[] writeDouble(double value) {
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    
    int intValue = (int) value;

    if (intValue == value) {
      if (intValue == 0) {
        bout.write((byte) BC_DOUBLE_ZERO);
        return bout.toByteArray();
      }
      else if (intValue == 1) {
        bout.write((byte) BC_DOUBLE_ONE);
        return bout.toByteArray();
      }
      else if (-0x80 <= intValue && intValue < 0x80) {
        bout.write((byte) BC_DOUBLE_BYTE);
        bout.write((byte) intValue);
        return bout.toByteArray();
      }
      else if (-0x8000 <= intValue && intValue < 0x8000) {
        bout.write((byte) BC_DOUBLE_SHORT);
        bout.write((byte) (intValue >> 8));
        bout.write((byte) intValue);
        return bout.toByteArray();
      }
    }

    int mills = (int) (value * 1000);

    if (0.001 * mills == value) {
      bout.write((byte) (BC_DOUBLE_MILL));
      bout.write((byte) (mills >> 24));
      bout.write((byte) (mills >> 16));
      bout.write((byte) (mills >> 8));
      bout.write((byte) (mills));
      return bout.toByteArray();
    }

    long bits = Double.doubleToLongBits(value);

    bout.write((byte) 'D');
    bout.write((byte) (bits >> 56));
    bout.write((byte) (bits >> 48));
    bout.write((byte) (bits >> 40));
    bout.write((byte) (bits >> 32));
    bout.write((byte) (bits >> 24));
    bout.write((byte) (bits >> 16));
    bout.write((byte) (bits >> 8));
    bout.write((byte) (bits));

    return bout.toByteArray();
  }
  
  /**
   * Writes a string value to the stream using UTF-8 encoding.
   * The string will be written with the following syntax:
   *
   * <code><pre>
   * S b16 b8 string-value
   * </pre></code>
   *
   * If the value is null, it will be written as
   *
   * <code><pre>
   * N
   * </pre></code>
   *
   * @param value the string value to write.
   */
  public static byte[] writeString(String value) {
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    
    if (value == null) {
      bout.write((byte) 'N');
      return bout.toByteArray();
    }
        
    int length = value.length();
    int strOffset = 0;

    while (length > 0x8000) {
      int sublen = 0x8000;

      // chunk can't end in high surrogate
      char tail = value.charAt(strOffset + sublen - 1);

      if (0xd800 <= tail && tail <= 0xdbff)
        sublen--;

      bout.write((byte) BC_STRING_CHUNK);
      bout.write((byte) (sublen >> 8));
      bout.write((byte) (sublen));
      
      try {
        bout.write(printString(value, strOffset, sublen));
      } catch(IOException ex) {
        throw new RuntimeException(ex);
      }

      length -= sublen;
      strOffset += sublen;
    }

    if (length <= STRING_DIRECT_MAX) {
      bout.write((byte) (BC_STRING_DIRECT + length));
    }
    else if (length <= STRING_SHORT_MAX) {
      bout.write((byte) (BC_STRING_SHORT + (length >> 8)));
      bout.write((byte) (length));
    }
    else {
      bout.write((byte) ('S'));
      bout.write((byte) (length >> 8));
      bout.write((byte) (length));
    }

    try {
      bout.write(printString(value, strOffset, length));
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    
    return bout.toByteArray();
    
  }
  
  /**
   * Prints a string to the stream, encoded as UTF-8
   *
   * @param v the string to print.
   */
  private static byte[] printString(String v, int strOffset, int length) {
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    
    for (int i = 0; i < length; i++) {
    
      char ch = v.charAt(i + strOffset);

      if (ch < 0x80)
        bout.write((byte) (ch));
      else if (ch < 0x800) {
        bout.write((byte) (0xc0 + ((ch >> 6) & 0x1f)));
        bout.write((byte) (0x80 + (ch & 0x3f)));
      }
      else {
        bout.write((byte) (0xe0 + ((ch >> 12) & 0xf)));
        bout.write((byte) (0x80 + ((ch >> 6) & 0x3f)));
        bout.write((byte) (0x80 + (ch & 0x3f)));
      }
    }
    
    return bout.toByteArray();
  }
  
  /**
   * Writes a byte array to the stream.
   * The array will be written with the following syntax:
   *
   * <code><pre>
   * B b16 b18 bytes
   * </pre></code>
   *
   * If the value is null, it will be written as
   *
   * <code><pre>
   * N
   * </pre></code>
   *
   * @param value the string value to write.
   */
  public static byte[] writeBytes(byte []buffer) {
    return writeBytes(buffer, 0, buffer.length);
  }
  
  /**
   * Writes a byte array to the stream.
   * The array will be written with the following syntax:
   *
   * <code><pre>
   * B b16 b18 bytes
   * </pre></code>
   *
   * If the value is null, it will be written as
   *
   * <code><pre>
   * N
   * </pre></code>
   *
   * @param value the string value to write.
   */
  public static byte[] writeBytes(byte []buffer, int offset, int length) {
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    
    int tmpOffset = 0;
    
    if (buffer == null) {
      bout.write((byte) 'N');
    } else {
    
      while(SIZE - tmpOffset - 3 < length) {
        
        int sublen = SIZE - tmpOffset - 3;
        
        bout.write((byte) BC_BINARY_CHUNK);
        bout.write((byte) (sublen >> 8));
        bout.write((byte) sublen);
        bout.write(buffer, tmpOffset, sublen);
        
        tmpOffset += sublen;

        length -= sublen;
        offset += sublen;
      }

      if (length <= BINARY_DIRECT_MAX) {
        bout.write((byte) (BC_BINARY_DIRECT + length));
      }
      else if (length <= BINARY_SHORT_MAX) {
        bout.write((byte) (BC_BINARY_SHORT + (length >> 8)));
        bout.write((byte) (length));
      }
      else {
        bout.write((byte) 'B');
        bout.write((byte) (length >> 8));
        bout.write((byte) (length));
      }

      bout.write(buffer, tmpOffset, length);
    }
    
    return bout.toByteArray();
  }
  
  
  
  private static StringBuilder _sbuf = new StringBuilder();
  
  // true if this is the last chunk
  private static boolean _isLastChunk;
  // the chunk length
  private static int _chunkLength;
  
  /**
   * Reads an arbitrary object from the input stream when the type
   * is unknown.
   */
  public static Object readObject(InputStream in) throws IOException {
    
    int tag = in.read();

    switch (tag) {
      case 'N':
        return null;

      case 'T':
        return Boolean.valueOf(true);

      case 'F':
        return Boolean.valueOf(false);

        // direct integer
      case 0x80: case 0x81: case 0x82: case 0x83:
      case 0x84: case 0x85: case 0x86: case 0x87:
      case 0x88: case 0x89: case 0x8a: case 0x8b:
      case 0x8c: case 0x8d: case 0x8e: case 0x8f:

      case 0x90: case 0x91: case 0x92: case 0x93:
      case 0x94: case 0x95: case 0x96: case 0x97:
      case 0x98: case 0x99: case 0x9a: case 0x9b:
      case 0x9c: case 0x9d: case 0x9e: case 0x9f:

      case 0xa0: case 0xa1: case 0xa2: case 0xa3:
      case 0xa4: case 0xa5: case 0xa6: case 0xa7:
      case 0xa8: case 0xa9: case 0xaa: case 0xab:
      case 0xac: case 0xad: case 0xae: case 0xaf:

      case 0xb0: case 0xb1: case 0xb2: case 0xb3:
      case 0xb4: case 0xb5: case 0xb6: case 0xb7:
      case 0xb8: case 0xb9: case 0xba: case 0xbb:
      case 0xbc: case 0xbd: case 0xbe: case 0xbf:
        return Integer.valueOf(tag - BC_INT_ZERO);

        /* byte int */
      case 0xc0: case 0xc1: case 0xc2: case 0xc3:
      case 0xc4: case 0xc5: case 0xc6: case 0xc7:
      case 0xc8: case 0xc9: case 0xca: case 0xcb:
      case 0xcc: case 0xcd: case 0xce: case 0xcf:
        return Integer.valueOf(((tag - BC_INT_BYTE_ZERO) << 8) + in.read());

        /* short int */
      case 0xd0: case 0xd1: case 0xd2: case 0xd3:
      case 0xd4: case 0xd5: case 0xd6: case 0xd7:
        return Integer.valueOf(((tag - BC_INT_SHORT_ZERO) << 16)
                               + 256 * in.read() + in.read());

      case 'I':
        return Integer.valueOf(parseInt(in));

        // direct long
      case 0xd8: case 0xd9: case 0xda: case 0xdb:
      case 0xdc: case 0xdd: case 0xde: case 0xdf:

      case 0xe0: case 0xe1: case 0xe2: case 0xe3:
      case 0xe4: case 0xe5: case 0xe6: case 0xe7:
      case 0xe8: case 0xe9: case 0xea: case 0xeb:
      case 0xec: case 0xed: case 0xee: case 0xef:
        return Long.valueOf(tag - BC_LONG_ZERO);

        /* byte long */
      case 0xf0: case 0xf1: case 0xf2: case 0xf3:
      case 0xf4: case 0xf5: case 0xf6: case 0xf7:
      case 0xf8: case 0xf9: case 0xfa: case 0xfb:
      case 0xfc: case 0xfd: case 0xfe: case 0xff:
        return Long.valueOf(((tag - BC_LONG_BYTE_ZERO) << 8) + in.read());

        /* short long */
      case 0x38: case 0x39: case 0x3a: case 0x3b:
      case 0x3c: case 0x3d: case 0x3e: case 0x3f:
        return Long.valueOf(((tag - BC_LONG_SHORT_ZERO) << 16) + 256 * in.read() + in.read());

      case BC_LONG_INT:
        return Long.valueOf(parseInt(in));

      case 'L':
        return Long.valueOf(parseLong(in));

      case BC_DOUBLE_ZERO:
        return Double.valueOf(0);

      case BC_DOUBLE_ONE:
        return Double.valueOf(1);

      case BC_DOUBLE_BYTE:
        return Double.valueOf((byte) in.read());

      case BC_DOUBLE_SHORT:
        return Double.valueOf((short) (256 * in.read() + in.read()));

      case BC_DOUBLE_MILL:
        {
          int mills = parseInt(in);

          return Double.valueOf(0.001 * mills);
        }

      case 'D':
        return Double.valueOf(parseDouble(in));

      case BC_DATE:
        return new Date(parseLong(in));

      case BC_DATE_MINUTE:
        return new Date(parseInt(in) * 60000L);

      case BC_STRING_CHUNK:
      case 'S':
        {
          _isLastChunk = tag == 'S';
          _chunkLength = (in.read() << 8) + in.read();

          _sbuf.setLength(0);

          parseString(_sbuf, in);

          return _sbuf.toString();
        }

      case 0x00: case 0x01: case 0x02: case 0x03:
      case 0x04: case 0x05: case 0x06: case 0x07:
      case 0x08: case 0x09: case 0x0a: case 0x0b:
      case 0x0c: case 0x0d: case 0x0e: case 0x0f:

      case 0x10: case 0x11: case 0x12: case 0x13:
      case 0x14: case 0x15: case 0x16: case 0x17:
      case 0x18: case 0x19: case 0x1a: case 0x1b:
      case 0x1c: case 0x1d: case 0x1e: case 0x1f:
        {
          _isLastChunk = true;
          _chunkLength = tag - 0x00;

          _sbuf.setLength(0);

          parseString(_sbuf, in);

          return _sbuf.toString();
        }

      case 0x30: case 0x31: case 0x32: case 0x33:
        {
          _isLastChunk = true;
          _chunkLength = (tag - 0x30) * 256 + in.read();

          _sbuf.setLength(0);

          parseString(_sbuf, in);

          return _sbuf.toString();
        }

      case BC_BINARY_CHUNK:
      case 'B':
      {
        _isLastChunk = tag == 'B';
        _chunkLength = (in.read() << 8) + in.read();

        int data;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
      
        while ((data = parseByte(in)) >= 0)
          bos.write(data);

        return bos.toByteArray();
      }
      case 0x20: case 0x21: case 0x22: case 0x23:
      case 0x24: case 0x25: case 0x26: case 0x27:
      case 0x28: case 0x29: case 0x2a: case 0x2b:
      case 0x2c: case 0x2d: case 0x2e: case 0x2f:
        {
          _isLastChunk = true;
          int len = tag - 0x20;
          _chunkLength = 0;

          byte []data = new byte[len];

          for (int i = 0; i < len; i++)
            data[i] = (byte) in.read();

          return data;
        }

      case 0x34: case 0x35: case 0x36: case 0x37:
        {
          _isLastChunk = true;
          int len = (tag - 0x34) * 256 + in.read();
          _chunkLength = 0;

          byte []buffer = new byte[len];

          for (int i = 0; i < len; i++) {
            buffer[i] = (byte) in.read();
          }

          return buffer;
        }


      default:
        if (tag < 0)
          throw new RuntimeException("readObject: unexpected end of file");
        else
          throw new RuntimeException("readObject: unknown code '" + tag + "'");
    }
  }
  
  /**
   * Parses a 32-bit integer value from the stream.
   *
   * <pre>
   * b32 b24 b16 b8
   * </pre>
   */
  private static int parseInt(InputStream in) throws IOException {
    int b32 = in.read();
    int b24 = in.read();
    int b16 = in.read();
    int b8 = in.read();
    return (b32 << 24) + (b24 << 16) + (b16 << 8) + b8;
  }
  
  /**
   * Parses a 64-bit long value from the stream.
   *
   * <pre>
   * b64 b56 b48 b40 b32 b24 b16 b8
   * </pre>
   */
  private static long parseLong(InputStream in) throws IOException {
    long b64 = in.read();
    long b56 = in.read();
    long b48 = in.read();
    long b40 = in.read();
    long b32 = in.read();
    long b24 = in.read();
    long b16 = in.read();
    long b8 = in.read();

    return ((b64 << 56)
            + (b56 << 48)
            + (b48 << 40)
            + (b40 << 32)
            + (b32 << 24)
            + (b24 << 16)
            + (b16 << 8)
            + b8);
  }
  
  /**
   * Parses a 64-bit double value from the stream.
   *
   * <pre>
   * b64 b56 b48 b40 b32 b24 b16 b8
   * </pre>
   */
  private static double parseDouble(InputStream in) throws IOException {
    long bits = parseLong(in);
    return Double.longBitsToDouble(bits);
  }
  
  private static void parseString(StringBuilder sbuf, InputStream in) throws IOException {
    while (true) {
      if (_chunkLength <= 0) {
        if (! parseChunkLength(in))
          return;
      }
      
      int length = _chunkLength;
      _chunkLength = 0;
      
      while (length-- > 0) {
        sbuf.append((char) parseUTF8Char(in));
      }
    }
  }

  private static boolean parseChunkLength(InputStream in) throws IOException {
    if (_isLastChunk)
      return false;

    int code = in.read();

    switch (code) {
    case BC_STRING_CHUNK:
      _isLastChunk = false;

      _chunkLength = (in.read() << 8) + in.read();
      break;
      
    case 'S':
      _isLastChunk = true;

      _chunkLength = (in.read() << 8) + in.read();
      break;

    case 0x00: case 0x01: case 0x02: case 0x03:
    case 0x04: case 0x05: case 0x06: case 0x07:
    case 0x08: case 0x09: case 0x0a: case 0x0b:
    case 0x0c: case 0x0d: case 0x0e: case 0x0f:

    case 0x10: case 0x11: case 0x12: case 0x13:
    case 0x14: case 0x15: case 0x16: case 0x17:
    case 0x18: case 0x19: case 0x1a: case 0x1b:
    case 0x1c: case 0x1d: case 0x1e: case 0x1f:
      _isLastChunk = true;
      _chunkLength = code - 0x00;
      break;

    case 0x30: case 0x31: case 0x32: case 0x33:
      _isLastChunk = true;
      _chunkLength = (code - 0x30) * 256 + in.read();
      break;

    default:
      throw new RuntimeException("Unable to parse chunk length");
    }

    return true;
  }

  /**
   * Parses a single UTF8 character.
   */
  private static int parseUTF8Char(InputStream in) throws IOException {
    int ch = in.read();

    if (ch < 0x80)
      return ch;
    else if ((ch & 0xe0) == 0xc0) {
      int ch1 = in.read();
      int v = ((ch & 0x1f) << 6) + (ch1 & 0x3f);

      return v;
    }
    else if ((ch & 0xf0) == 0xe0) {
      int ch1 = in.read();
      int ch2 = in.read();
      int v = ((ch & 0x0f) << 12) + ((ch1 & 0x3f) << 6) + (ch2 & 0x3f);

      return v;
    }
    else
      throw new RuntimeException("bad utf-8 encoding at '" + ch + "'");
  }
  
  /**
   * Reads a byte from the underlying stream.
   */
  private static int parseByte(InputStream is) throws IOException
  {
    while (_chunkLength <= 0) {
      if (_isLastChunk) {
        return -1;
      }

      int code = is.read();

      switch (code) {
      case BC_BINARY_CHUNK:
        _isLastChunk = false;

        _chunkLength = (is.read() << 8) + is.read();
        break;
        
      case 'B':
        _isLastChunk = true;

        _chunkLength = (is.read() << 8) + is.read();
        break;

      case 0x20: case 0x21: case 0x22: case 0x23:
      case 0x24: case 0x25: case 0x26: case 0x27:
      case 0x28: case 0x29: case 0x2a: case 0x2b:
      case 0x2c: case 0x2d: case 0x2e: case 0x2f:
        _isLastChunk = true;

        _chunkLength = code - 0x20;
        break;

      case 0x34: case 0x35: case 0x36: case 0x37:
        _isLastChunk = true;
        _chunkLength = (code - 0x34) * 256 + is.read();
        break;

      default:
        throw new RuntimeException("Unknown code '" + code + "' while parsing byte");
      }
    }

    _chunkLength--;

    return is.read();
  }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.holiday69.tinydb.bitcask.file.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.holiday69.tinydb.bitcask.vo.Key;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.TreeSet;

/**
 *
 * @author Stefano Fratini <mail@stefanofratini.it>
 */
public class KryoUtils {
  
  private final static Double DOUBLE_PRECISION = 1000.0;
  
  /**
   * Writes a boolean value to the stream.
   * @param value the boolean value to write.
   */
  
  public static byte[] writeBoolean(boolean value) {
    
    byte[] ret = new byte[1];
    Output output = new Output(ret);
    output.writeBoolean(value);
    output.close();
    return ret;
  }
  
  /**
   * Writes an integer value to the stream.
   * @param value the integer value to write.
   */
  
  public static byte[] writeInt(int value){
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream(5);
    Output output = new Output(bos);
    output.writeInt(value, true);
    output.close();
    return bos.toByteArray();
  }
  
  
   /**
   * Writes a long value to the stream.
   * @param value the long value to write.
   */
  public static byte[] writeLong(long value)
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(9);
    Output output = new Output(bos);
    output.writeLong(value, true);
    output.close();
    return bos.toByteArray();
  }
  
  /**
   * Writes a double value to the stream.
   * @param value the double value to write.
   */
  public static byte[] writeDouble(double value) {
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream(9);
    Output output = new Output(bos);
    output.writeDouble(value, DOUBLE_PRECISION, true);
    output.close();
    return bos.toByteArray();
  }
  
  /**
   * Writes a string value to the stream using UTF-8 encoding.
   * @param value the string value to write.
   */
  public static byte[] writeString(String value) {
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream(9);
    Output output = new Output(bos);
    output.writeString(value);
    output.close();
    return bos.toByteArray();
    
  }
  
  /**
   * Writes bytes to the stream.
   * @param buffer the buffer to copy data from
   */
  public static byte[] writeBytes(byte []buffer) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(9);
    Output output = new Output(bos);
    output.writeBytes(buffer);
    output.close();
    return bos.toByteArray();
  }
  
  /**
   * Writes an arbitrary object to the output stream when the type
   * is unknown.
   */
  public static byte[] writeClassAndObject(Object obj) {
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream(9);
    Output output = new Output(bos);
    getKryo().writeClassAndObject(output, obj);
    output.close();
    return bos.toByteArray();
  }
  
  /**
   * Reads an arbitrary object from the input stream when the type
   * is unknown.
   */
  public static Object readClassAndObject(InputStream in) {
    Input input = new Input(in, 1);
    
    return getKryo().readClassAndObject(input);
  }
  
  private static Kryo getKryo() {
    Kryo kryo = new Kryo();
    kryo.register(Key.class);
    kryo.register(TreeSet.class);
    return kryo;
  }
  
  /**
   * Parses a 1 byte boolean from the stream
   *
   */
  public static boolean readBoolean(InputStream in) {
    Input input = new Input(in, 1);
    return input.readBoolean();
  }
  
  /**
   * Parses a 32-bit integer value from the stream.
   *
   */
  public static int readInt(InputStream in) {
    Input input = new Input(in, 1);
    return input.readInt(true);
  }
  
  /**
   * Parses a 64-bit long value from the stream.
   */
  public static long readLong(InputStream in) {
    Input input = new Input(in, 1);
    long ret = input.readLong(true);
    return ret;
  }
  
  /**
   * Parses a 1.0 precision double value from the stream.
   */
  public static double readDouble(InputStream in) {
    Input input = new Input(in, 1);
    return input.readDouble(DOUBLE_PRECISION, true);
  }
  
  public static String readString(InputStream in){
    Input input = new Input(in, 1);
    return input.readString();
  }
  
  /**
   * Reads a byte from the underlying stream.
   */
  public static int readByte(InputStream in) {
    Input input = new Input(in, 1);
    return input.readByte();
  }
}

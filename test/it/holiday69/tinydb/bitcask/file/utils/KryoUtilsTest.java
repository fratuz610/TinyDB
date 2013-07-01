/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.utils;

import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Stefano
 */
public class KryoUtilsTest {
  
  public KryoUtilsTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of writeBoolean method, of class KryoUtils.
   */
  @Test
  public void testBoolean() {
    System.out.println("testBoolean");
    boolean value = false;
    
    byte[] result = KryoUtils.writeBoolean(value);
    boolean readBack = KryoUtils.readBoolean(new ByteArrayInputStream(result));
    assertEquals(readBack, value);
  }

  /**
   * Test of writeInt method, of class KryoUtils.
   */
  @Test
  public void testInt() {
    System.out.println("testInt");
    int value = 1234567;
    
    byte[] result = KryoUtils.writeInt(value);
    int readBack = KryoUtils.readInt(new ByteArrayInputStream(result));
    assertEquals(readBack, value);
  }

  /**
   * Test of testLong method, of class KryoUtils.
   */
  @Test
  public void testLong() {
    System.out.println("testLong");
    long value = 123456789l;
    
    byte[] result = KryoUtils.writeLong(value);
    long readBack = KryoUtils.readLong(new ByteArrayInputStream(result));
    assertEquals(readBack, value);
  }

  /**
   * Test of writeDouble method, of class KryoUtils.
   */
  @Test
  public void testDouble() {
    System.out.println("testDouble");
    double value = 123.678;
    byte[] result = KryoUtils.writeDouble(value);
    double readBack = KryoUtils.readDouble(new ByteArrayInputStream(result));
    assertEquals(value, readBack, 0.0);
  }

  /**
   * Test of writeString method, of class KryoUtils.
   */
  @Test
  public void testString() {
    System.out.println("testString");
    String value = "Hello World here I am";
    byte[] result = KryoUtils.writeString(value);
    String readBack = KryoUtils.readString(new ByteArrayInputStream(result));
    assertEquals(readBack, value);
  }

  /**
   * Test of writeBytes method, of class KryoUtils.
   */
  @Test
  public void testBytes() {
    System.out.println("testBytes");
    byte[] value = "Hello World here I am".getBytes();
    byte[] result = KryoUtils.writeBytes(value);
    assertEquals(value.length, result.length);
  }

  /**
   * Test of readClassAndObject method, of class KryoUtils.
   */
  @Test
  public void testClassAndObject() {
    System.out.println("testClassAndObject");
    String value = new String("hello");
    byte[] result = KryoUtils.writeClassAndObject(value);
    System.out.println("'hello' string serialized in " + result.length + " bytes");
    String readBack = (String) KryoUtils.readClassAndObject(new ByteArrayInputStream(result));
    assertEquals(value, readBack);
  }
  
  /**
   * Test of readClassAndObject method, of class KryoUtils.
   */
  @Test
  public void testList() {
    System.out.println("testClassAndObject");
    List<String> value = new LinkedList<String>();
    value.add("Hello");
    value.add("world");
    byte[] result = KryoUtils.writeClassAndObject(value);
    System.out.println("Sample list serialized in " + result.length + " bytes");
    LinkedList<String> readBack = (LinkedList) KryoUtils.readClassAndObject(new ByteArrayInputStream(result));
    assertEquals(value, readBack);
  }

}

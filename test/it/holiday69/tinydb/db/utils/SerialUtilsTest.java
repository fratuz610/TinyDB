/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.utils;

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
public class SerialUtilsTest {
  
  public SerialUtilsTest() {
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
   * Test of longToByteArray method, of class SerialUtils.
   */
  @Test
  public void testLongToByteArray_long() {
    System.out.println("longToByteArray");
    long value = 12345L;
    
    byte[] result = SerialUtils.longToByteArray(value);
    assertEquals(8, result.length);
    long computedBack = SerialUtils.byteArrayToLong(result);
    
    assertEquals(value, computedBack);
  }

  /**
   * Test of longToByteArray method, of class SerialUtils.
   */
  @Test
  public void testLongToByteArray_long_int() {
    System.out.println("longToByteArray");
    long value = 12345L;
    
    byte[] result = SerialUtils.longToByteArray(value, 4);
    assertEquals(4, result.length);
    long computedBack = SerialUtils.byteArrayToLong(result);
    
    assertEquals(value, computedBack);
  }

  /**
   * Test of intToByteArray method, of class SerialUtils.
   */
  @Test
  public void testIntToByteArray_int() {
    System.out.println("intToByteArray");
    int value = 1235;
    
    byte[] result = SerialUtils.intToByteArray(value);
    assertEquals(4, result.length);
    int computedBack = SerialUtils.byteArrayToInt(result);
    
    assertEquals(value, computedBack);
  }

  /**
   * Test of intToByteArray method, of class SerialUtils.
   */
  @Test
  public void testIntToByteArray_long_int() {
    System.out.println("intToByteArray");
    int value = 1235;
    
    byte[] result = SerialUtils.intToByteArray(value, 3);
    assertEquals(3, result.length);
    int computedBack = SerialUtils.byteArrayToInt(result);
    
    assertEquals(value, computedBack);
  }
  
}

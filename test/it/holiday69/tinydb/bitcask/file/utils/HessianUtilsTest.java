/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.utils;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Date;
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
public class HessianUtilsTest {
  
  public HessianUtilsTest() {
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
   * Test of writeInt method, of class HessianUtils.
   */
  @Test
  public void testWriteInt() {
    System.out.println("writeInt");
    int value = (int)(Math.random()*10000);
    byte[] result = HessianUtils.writeInt(value);
    
    System.out.println("value: " + value +" => " + Arrays.toString(result));
    assertEquals(value, HessianUtils.readObject(new ByteArrayInputStream(result)));
  }
  
  @Test
  public void testWriteLong() {
    System.out.println("writeLong");
    long value = (long)(Math.random()*10000000);
    byte[] result = HessianUtils.writeLong(value);
    
    System.out.println("value: " + value +" => " + Arrays.toString(result));
    assertEquals(value, HessianUtils.readObject(new ByteArrayInputStream(result)));
  }
  
  @Test
  public void testWriteDouble() {
    System.out.println("writeDouble");
    double value = Math.random()*100000;
    byte[] result = HessianUtils.writeDouble(value);
    System.out.println("value: " + value +" => " + Arrays.toString(result));
    assertEquals(value, HessianUtils.readObject(new ByteArrayInputStream(result)));
  }
  
  @Test
  public void testWriteUTCDate() {
    System.out.println("writeUTCDate");
    long value = new Date().getTime();
    byte[] result = HessianUtils.writeUTCDate(value);
    System.out.println("value: " + value +" => " + Arrays.toString(result));
    assertEquals(value, ((Date)HessianUtils.readObject(new ByteArrayInputStream(result))).getTime());
  }
  
  @Test
  public void testWriteString() {
    System.out.println("writeString");
    String value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In elementum iaculis aliquet. Proin libero nibh, fermentum in ornare sed, pulvinar eu ipsum. Vestibulum tristique, sapien quis rutrum ornare, nunc mauris imperdiet augue, ultrices euismod sapien velit sed metus. Aenean nec nulla mi. Vestibulum sed velit id leo laoreet vestibulum pretium ut neque. Donec dictum augue non risus molestie aliquet. Sed hendrerit mattis leo, quis scelerisque ipsum ultrices in.";
    int startIndex = (int)(Math.random()*10000);
    int endIndex = startIndex + (int)(Math.random()*10000);
    
    if(startIndex >= value.length()) startIndex = 0;
    if(endIndex > value.length()) endIndex = value.length();
    
    value = value.substring(startIndex, endIndex);
    
    byte[] result = HessianUtils.writeString(value);
    System.out.println("value: " + value +" => " + Arrays.toString(result));
    assertEquals(value, HessianUtils.readObject(new ByteArrayInputStream(result)));
  }

}

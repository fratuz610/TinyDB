/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.keydir.vo;

import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.bitcask.vo.Key.Type;
import it.holiday69.tinydb.db.utils.SerialUtils;
import java.io.ByteArrayOutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Stefano
 */
public class KeyTest {
  
  public KeyTest() {
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
   * Test of fromString method, of class Key.
   */
  @Test
  public void testFromString() {
    System.out.println("fromString");
    String src = "hello world";
    
    Key result = new Key().fromString(src);
    assertEquals(result.getType(), Key.Type.STRING);
    assertEquals(result.keyValue(), "hello world");
    
    System.out.println(result.toByteArrayString());
  }

  
  @Test
  public void testFromLong() {
    System.out.println("fromLong");
    long src = 12345l;
    
    Key result = new Key().fromLong(src);
    assertEquals(result.getType(), Key.Type.LONG);
    assertEquals(result.keyValue(), 12345l);
    
    System.out.println(result.toByteArrayString());
  }
  
  @Test
  public void testFromByteArrayString() {
    System.out.println("fromByteArrayString");
    byte[] src = {(byte)0, 'h', 'i'};
    
    Key result = new Key().fromByteArray(src);
    assertEquals(result.getType(), Key.Type.STRING);
    assertEquals(result.keyValue(), "hi");
    
    System.out.println(result.toByteArrayString());
  }
  
  @Test
  public void testFromByteArrayLong() {
    System.out.println("fromByteArrayLong");
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try {
      bout.write((byte) 1);
      bout.write(SerialUtils.longToByteArray(123456l));
    } catch(Throwable th) { } 
    
    Key result = new Key().fromByteArray(bout.toByteArray());
    assertEquals(result.getType(), Key.Type.LONG);
    assertEquals(result.keyValue(), 123456l);
    
    System.out.println(result.toByteArrayString());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testUnknownFormat() {
    System.out.println("testUnknownFormat");
    
    byte[] src = {(byte)2, 'h', 'i'};
    
    new Key().fromByteArray(src); 
  }
  
}

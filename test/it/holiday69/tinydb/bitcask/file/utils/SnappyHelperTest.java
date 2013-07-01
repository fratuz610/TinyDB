/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Stefano
 */
public class SnappyHelperTest {
  
  public SnappyHelperTest() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of compress method, of class SnappyHelper.
   */
  @Test
  public void testCompress() {
    System.out.println("compress");
    byte[] source = "this is an example this is an example this is an example this is an example this is an example this is an example this is an example this is an example".getBytes();
    SnappyHelper instance = new SnappyHelper();
    byte[] result = instance.compress(source);
    
    System.out.println("Compressed Size: " + result.length + " vs uncompressed size: " + source.length);
    
    byte[] sourceBack = instance.uncompress(result);
    assertArrayEquals(source, sourceBack);
  }

}

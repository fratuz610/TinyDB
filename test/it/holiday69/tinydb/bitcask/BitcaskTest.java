/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask;

import it.holiday69.tinydb.bitcask.vo.Key;
import java.util.Date;
import java.util.concurrent.TimeUnit;
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
public class BitcaskTest {
  
  public BitcaskTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  protected Bitcask _bitcask;
  
  @Before
  public void setUp() {
    _bitcask = new Bitcask("hello-world", new BitcaskOptions()
            .withDbFolder(System.getProperty("java.io.tmpdir"))
            .withCompactEvery(5, TimeUnit.SECONDS)
            .withRecordPerFile(10));
  }
  
  @After
  public void tearDown() {
    _bitcask.shutdown(false);
  }

  /**
   * General test method (put, get and delete)
   */
  @Test
  public void testBitcask() {
    System.out.println("testBitcask");
    
    Date date = new Date();
    Key key = new Key().fromString("test");
    
    _bitcask.put(key, date);
    
    Date newDate = (Date) _bitcask.get(new Key().fromString("test"));
    
    // we make sure we get the same value
    assertEquals(date, newDate);
    
    // we delete setting to null
    _bitcask.put(key, null);
    
    Object nullObject = _bitcask.get(key);
    
    assertNull(nullObject);
    
  }

}

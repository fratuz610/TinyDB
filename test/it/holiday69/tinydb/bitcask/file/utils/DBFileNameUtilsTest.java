/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.utils;

import java.io.File;
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
public class DBFileNameUtilsTest {
  
  public DBFileNameUtilsTest() {
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
   * Test of getDBFile method, of class DBFileNameUtils.
   */
  @Test
  public void testGetDBFile() {
    System.out.println("getDBFile");
    File dbFolder = new File(".");
    String dbName = "db-name";
    int index = 123;
    
    File result = DBFileUtils.getDBFile(dbFolder, dbName, index);
    assertEquals(result.getName(), "db-name.db.123");
  }

  /**
   * Test of getHintFile method, of class DBFileNameUtils.
   */
  @Test
  public void testGetHintFile() {
    System.out.println("getHintFile");
    File dbFolder = new File(".");
    String dbName = "db-name";
    
    File result = DBFileUtils.getHintFile(dbFolder, dbName);
    assertEquals(result.getName(), "db-name.db.hint");
  }

  /**
   * Test of getDBFileNumber method, of class DBFileNameUtils.
   */
  @Test
  public void testGetDBFileNumber() {
    System.out.println("getDBFileNumber");
    File dbFile = new File("db-name.db.0001232");
    Integer expResult = 1232;
    Integer result = DBFileUtils.getDBFileNumber(dbFile);
    assertEquals(expResult, result);
    
  }

  /**
   * Test of isValidDBFileName method, of class DBFileNameUtils.
   */
  @Test
  public void testIsValidDBFileName() {
    System.out.println("isValidDBFileName");
    String fileName = "test.db.0001";
    boolean result = DBFileUtils.isValidDBFileName(fileName);
    assertEquals(true, result);
  }
  
  @Test
  public void testInvalidDBFileName() {
    System.out.println("isValidDBFileName");
    String fileName = "test.dba.0001";
    boolean result = DBFileUtils.isValidDBFileName(fileName);
    assertEquals(false, result);
    
    fileName = "test.dba.0001";
    result = DBFileUtils.isValidDBFileName(fileName);
    assertEquals(false, result);
    
    fileName = "testdb.1";
    result = DBFileUtils.isValidDBFileName(fileName);
    assertEquals(false, result);
    
    fileName = "test.db";
    result = DBFileUtils.isValidDBFileName(fileName);
    assertEquals(false, result);
  }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm;

import it.holiday69.dataservice.DataService;
import it.holiday69.tinydb.jdbm.annotations.Id;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author fratuz610
 */
public class TinyDBDataServiceTest {
  
  public TinyDBDataServiceTest() {
  }

  public static class TestUser {
    
    @Id public String username;
    public String password;
    public List<String> stringList = new LinkedList<String>();
  }
  
  /**
   * Test of put method, of class TinyDBDataService.
   */
  @Test
  public void testPut() {
    
    DataService instance = new TinyDBDataService();
    
    TestUser newUser = new TestUser();
    newUser.username = "hello";
    newUser.password = "this is a password";
    newUser.stringList.add("Test string");
    
    System.out.println("putting one item");
    instance.put(newUser);
    
    System.out.println("retrieving saved item");
    TestUser testUser = instance.get("hello", TestUser.class);
    
    assertEquals(testUser.username, "hello");
    assertEquals(testUser.password, "hello");
    assertEquals(testUser.stringList.size(), 1);
  }

  /**
   * Test of putAll method, of class TinyDBDataService.
   */
  @Test
  public void testPutAll() {
    System.out.println("putAll");
    TinyDBDataService instance = new TinyDBDataService();
    instance.putAll(null);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of get method, of class TinyDBDataService.
   */
  @Test
  public void testGet_GenericType_Class() {
    System.out.println("get");
    TinyDBDataService instance = new TinyDBDataService();
    Object expResult = null;
    Object result = instance.get(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of get method, of class TinyDBDataService.
   */
  @Test
  public void testGet_3args() {
    System.out.println("get");
    TinyDBDataService instance = new TinyDBDataService();
    Object expResult = null;
    Object result = instance.get(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of get method, of class TinyDBDataService.
   */
  @Test
  public void testGet_Query_Class() {
    System.out.println("get");
    TinyDBDataService instance = new TinyDBDataService();
    Object expResult = null;
    Object result = instance.get(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of get method, of class TinyDBDataService.
   */
  @Test
  public void testGet_Class() {
    System.out.println("get");
    TinyDBDataService instance = new TinyDBDataService();
    Object expResult = null;
    Object result = instance.get(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getList method, of class TinyDBDataService.
   */
  @Test
  public void testGetList_3args() {
    System.out.println("getList");
    TinyDBDataService instance = new TinyDBDataService();
    List expResult = null;
    List result = instance.getList(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getList method, of class TinyDBDataService.
   */
  @Test
  public void testGetList_Class() {
    System.out.println("getList");
    TinyDBDataService instance = new TinyDBDataService();
    List expResult = null;
    List result = instance.getList(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getList method, of class TinyDBDataService.
   */
  @Test
  public void testGetList_Query_Class() {
    System.out.println("getList");
    TinyDBDataService instance = new TinyDBDataService();
    List expResult = null;
    List result = instance.getList(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of delete method, of class TinyDBDataService.
   */
  @Test
  public void testDelete() {
    System.out.println("delete");
    Object object = null;
    TinyDBDataService instance = new TinyDBDataService();
    instance.delete(object);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of deleteAll method, of class TinyDBDataService.
   */
  @Test
  public void testDeleteAll_Iterable() {
    System.out.println("deleteAll");
    TinyDBDataService instance = new TinyDBDataService();
    //instance.deleteAll(null);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of deleteAll method, of class TinyDBDataService.
   */
  @Test
  public void testDeleteAll_Query_Class() {
    System.out.println("deleteAll");
    TinyDBDataService instance = new TinyDBDataService();
    //instance.deleteAll(null);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of deleteAll method, of class TinyDBDataService.
   */
  @Test
  public void testDeleteAll_Class() {
    System.out.println("deleteAll");
    TinyDBDataService instance = new TinyDBDataService();
    //instance.deleteAll(null);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getResultSetSize method, of class TinyDBDataService.
   */
  @Test
  public void testGetResultSetSize_Class() {
    System.out.println("getResultSetSize");
    TinyDBDataService instance = new TinyDBDataService();
    long expResult = 0L;
    long result = instance.getResultSetSize(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getResultSetSize method, of class TinyDBDataService.
   */
  @Test
  public void testGetResultSetSize_3args() {
    System.out.println("getResultSetSize");
    TinyDBDataService instance = new TinyDBDataService();
    long expResult = 0L;
    long result = instance.getResultSetSize(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getResultSetSize method, of class TinyDBDataService.
   */
  @Test
  public void testGetResultSetSize_Query_Class() {
    System.out.println("getResultSetSize");
    TinyDBDataService instance = new TinyDBDataService();
    long expResult = 0L;
    long result = instance.getResultSetSize(null);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of performMaintenance method, of class TinyDBDataService.
   */
  @Test
  public void testPerformMaintenance() {
    System.out.println("performMaintenance");
    TinyDBDataService instance = new TinyDBDataService();
    instance.performMaintenance();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }
}

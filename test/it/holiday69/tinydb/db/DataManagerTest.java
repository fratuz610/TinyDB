/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.tinydb.db.entity.RecordRef;
import java.io.File;
import java.io.Serializable;
import java.util.Date;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author fratuz610
 */
public class DataManagerTest {
  
  private DataManager _dataManager;
  
  public static class TestObj implements Serializable {
    public String name;
    public String surname;
    
  }
  
  public DataManagerTest() {
    _dataManager = new DataManager(new File("."), "test-db");
  }

  /**
   * Test of getRecord method, of class DataManager.
   */
  @Test
  public void testPutAndGet() {
    
    System.out.println("testPutAndGet");
    
    TestObj testObj = new TestObj();
    testObj.name = new Date().toString();
    testObj.surname = new Date().toString();
    
    RecordRef ref = _dataManager.putRecord(testObj);
    assertNotNull(ref);
    
    System.out.println("ref: " + ref);
    
    TestObj retObj = (TestObj) _dataManager.getRecord(ref);
    
    assertEquals(retObj.name, testObj.name);
    assertEquals(retObj.surname, testObj.surname);
  }

  /**
   * Test of putRecord method, of class DataManager.
   */
  @Test
  public void testBulkPut() {
    System.out.println("testBulkPut");
    
    TestObj firstObj = new TestObj();
    firstObj.name = new Date().toString();
    firstObj.surname = new Date().toString();
    
    RecordRef firstObjRef = _dataManager.putRecord(firstObj);
    
    for(int i = 1; i < 10000; i++) {
      TestObj testObj = new TestObj();
      testObj.name = new Date().toString();
      testObj.surname = new Date().toString();
      _dataManager.putRecord(testObj);
    }
    
    TestObj firstObjRead = (TestObj) _dataManager.getRecord(firstObjRef);
    
    assertEquals(firstObjRead.name, firstObj.name);
    assertEquals(firstObjRead.surname, firstObj.surname);
  }
}

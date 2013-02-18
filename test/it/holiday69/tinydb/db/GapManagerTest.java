/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.tinydb.db.entity.RecordRef;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author fratuz610
 */
public class GapManagerTest {
  
  public GapManagerTest() {
  }

  /**
   * Test of addGap method, of class GapManager.
   */
  @Test
  public void testAddOneGap() {
    System.out.println("addOneGap");
    RecordRef srcRef = new RecordRef();
    srcRef.offset = new Double(Math.random()*2000).intValue();
    srcRef.size = new Double(Math.random()*2000).intValue();
    GapManager instance = new GapManager(new File("."), "test-db");
    
    System.out.println("readAllGaps: " + instance.readAllGaps());
    
    instance.addGap(srcRef);
    
    System.out.println("readAllGaps: " + instance.readAllGaps());
    
    System.out.println("Adding a record ref of " + srcRef.toString());
    
    RecordRef readRef = instance.acquireGap(srcRef.size - 100);
    
    System.out.println("readAllGaps: " + instance.readAllGaps());
    
    assertEquals(readRef.offset, srcRef.offset);
    assertEquals(readRef.size, srcRef.size);
  }
  
  /**
   * Test of addGap method, of class GapManager.
   */
  @Test
  public void testAddMultipleGaps() {
    System.out.println("testAddMultipleGaps");
    
    List<RecordRef> recordRefList = new LinkedList<RecordRef>();
    
    GapManager instance = new GapManager(new File("."), "test-db");
    
    instance.clear();
    
    for(int i = 0; i < 3000; i++) {
      RecordRef srcRef = new RecordRef();
      srcRef.offset = new Double(Math.random()*2000).intValue();
      srcRef.size = new Double(Math.random()*2000).intValue();
      recordRefList.add(srcRef);
      instance.addGap(srcRef);
    }
    
    System.out.println("readAllGaps: items " + instance.readAllGaps().size());
    
    assertEquals(3000, instance.readAllGaps().size());
    
  }
}

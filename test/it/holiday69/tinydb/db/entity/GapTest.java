/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.entity;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author fratuz610
 */
public class GapTest {
  
  public GapTest() {
  }

  /**
   * Test of write method, of class Gap.
   */
  @Test
  public void testReadWrite() throws IOException {
    
    Kryo kryo = new Kryo();
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    Output output = new Output(bout);
    Gap instance = new Gap();
    instance.offset = new Double(Math.random()*2000).intValue();
    instance.size = new Double(Math.random()*2000).intValue();
    
    System.out.println("Created and written: " + instance);
    
    kryo.writeObject(output, instance);
    output.close();
    bout.close();
    
    byte[] written = bout.toByteArray();
    
    System.out.println("Bytes used: " + written.length);
    
    ByteArrayInputStream bin = new ByteArrayInputStream(written);
    Input input = new Input(bin);
    
    Gap readBack = kryo.readObject(input, Gap.class);
    
    System.out.println("Read back: " + readBack);
    
    assertEquals(instance.offset, readBack.offset);
    assertEquals(instance.size, readBack.size);
    
    
  }

}

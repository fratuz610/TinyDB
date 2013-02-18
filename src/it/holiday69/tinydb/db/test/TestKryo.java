/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import it.holiday69.tinydb.db.entity.RecordRef;
import java.io.ByteArrayOutputStream;

/**
 *
 * @author fratuz610
 */
public class TestKryo {
  
  public static class AANode {
    public int level = 1;
    public int leftAddr = -1;
    public int rightAddr = -1;
    public RecordRef value = null;
  }
  
  public static void main(String args[]) {
    
    AANode node = new AANode();
    node.level = 10000;
    node.leftAddr = 1234353;
    node.rightAddr = 1234353;
    
    Kryo kryo = new Kryo();
    kryo.register(AANode.class);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    Output output = new Output(bout);
    kryo.writeClassAndObject(output, node);
    output.flush();
    
    System.out.println("Serialized in "  + bout.toByteArray().length + " bytes");
  }
  
}

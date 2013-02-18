/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.entity;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;

/**
 *
 * @author fratuz610
 */
public class RecordRef implements KryoSerializable {
  
  public boolean deleted = false;
  public int offset;
  public int size;
  
  public boolean isNull() { return (offset == 0 && size == 0); }
  
  @Override
  public void write(Kryo kryo, Output output) {
    output.writeBoolean(deleted);
    output.writeInt(offset);
    output.writeInt(size);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    deleted = input.readBoolean();
    offset = input.readInt();
    size = input.readInt();
  }
  
  public static byte[] toByteArray(RecordRef src) {
    
    Kryo kryo = new Kryo();
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    Output output = new Output(bout);
    kryo.writeObject(output, src);
    output.close();
    
    return bout.toByteArray();
  }
  
  @Override
  public String toString() { return "[RecordRef] offset: " + offset + " size: " + size + " [/RecordRef]"; } 
  
}

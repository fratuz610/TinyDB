/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.entity;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 *
 * @author fratuz610
 */
public class Gap implements KryoSerializable  {

  public boolean deleted = false;
  public int offset;
  public int size;
  
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
  
  @Override
  public String toString() { return "[Gap] deleted: "+deleted+" offset: " + offset + " size: " + size + "[/Gap]"; }
  
  
  public static byte[] toByteArray(Gap src) {
    
    Kryo kryo = new Kryo();
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    Output output = new Output(bout);
    kryo.writeObject(output, src);
    output.close();
    
    return bout.toByteArray();
  }
  
  public static Gap fromByteArray(byte[] src) {
    
    Kryo kryo = new Kryo();
    
    ByteArrayInputStream bin = new ByteArrayInputStream(src);
    Input input = new Input(bin);

    return kryo.readObject(input, Gap.class);
    
  }
  
  public static Gap fromRecordRef(RecordRef ref) {
    Gap gap = new Gap();
    gap.deleted = false;
    gap.offset = ref.offset;
    gap.size = ref.size;
    return gap;
  }
  
}

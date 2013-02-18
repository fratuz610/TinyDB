/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.entity;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.holiday69.tinydb.jdbm.vo.Key;

/**
 *
 * @author fratuz610
 */
public class IndexTreeNode implements KryoSerializable {
  
  public int offset = 0;
  public int level = 1;
  public int right = -1;
  public int left = -1;
  public Key key;
  public RecordRef value;
  
  @Override
  public void write(Kryo kryo, Output output) {
    output.writeInt(offset);
    output.writeInt(level);
    output.writeInt(right);
    output.writeInt(left);
    kryo.writeObject(output, key);
    kryo.writeObject(output, value);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    offset = input.readInt();
    level = input.readInt();
    right = input.readInt();
    left = input.readInt();
    key = kryo.readObject(input, Key.class);
    value = kryo.readObject(input, RecordRef.class);
  }
  
  @Override
  public String toString() { return "[IndexTreeNode] offset: "+offset+
          " level: " + level + 
          " right: " + right + 
          " left: " + left+ 
          " key: " + key + 
          " value: " + value + " [/IndexTreeNode]"; } 
  
}

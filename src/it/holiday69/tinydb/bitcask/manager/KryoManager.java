/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.manager;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.bitcask.vo.Key;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class KryoManager {
  
  private final Logger _log = Logger.getLogger(KryoManager.class.getSimpleName());
  
  private BitcaskOptions _options;
  
  public KryoManager(BitcaskOptions options) {
    _options = options;
  }
  
  /**
   * Serializes an object into a byte array
   */
  public <T> byte[] serializeObject(T object) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(9);
    Output output = new Output(bos);
    getKryo().writeClassAndObject(output, object);
    output.close();
    return bos.toByteArray();
  }
  
  /**
   * Reads an arbitrary object from the input stream when the type
   * is unknown.
   */
  public Object deserializeObject(InputStream in) {
    Input input = new Input(in, 1);
    return getKryo().readClassAndObject(input);
  }
  
  private Kryo getKryo() {
    Kryo kryo = new Kryo();
    //kryo.setRegistrationRequired(true);
    
    // registers the some internal classes minimizing overlap
    kryo.register(Key.class, 126);
    kryo.register(TreeSet.class, 127);
    
    for(Class clazz : _options.classMap.keySet()) {
      int index = _options.classMap.get(clazz);
      kryo.register(clazz, index);
    }
    
    return kryo;
  }
  
  
  
}

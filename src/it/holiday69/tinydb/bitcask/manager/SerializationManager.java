/*
    Copyright 2013 Stefano Fratini (mail@stefanofratini.it)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package it.holiday69.tinydb.bitcask.manager;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.holiday69.tinydb.bitcask.vo.Key;
import it.holiday69.tinydb.log.DBLog;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class SerializationManager {
  
  private final DBLog _log = DBLog.getInstance(SerializationManager.class.getSimpleName());
  
  private final static Map<Class, Integer> _kryoClassMap = new HashMap<Class, Integer>();
  
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
  
  /**
   * Reads an arbitrary object from the input stream when the type
   * is unknown.
   */
  public Object deserializeObject(byte[] src) {
    Input input = new Input(src);
    return getKryo().readClassAndObject(input);
  }
  
  private Kryo getKryo() {
    Kryo kryo = new Kryo();
    //kryo.setRegistrationRequired(true);
    
    // registers the some internal classes minimizing overlap
    kryo.register(Key.class, 126);
    kryo.register(TreeSet.class, 127);
    
    synchronized(_kryoClassMap) {
      for(Class clazz : _kryoClassMap.keySet()) {
        int index = _kryoClassMap.get(clazz);
        kryo.register(clazz, index);
      }
    }
    
    return kryo;
  }
  
  public static final void mapClass(Class<?> clazz, int index) {
    
    if(index < 10)
      throw new IllegalArgumentException("Valid indexes are from 10");
    
    if(index == 126 || index == 127)
      throw new IllegalArgumentException("Indexed 126 and 127 are reserved and cannot be used");
        
     synchronized(_kryoClassMap) {
      _kryoClassMap.put(clazz, index);
     }
  }
  
  
  
}

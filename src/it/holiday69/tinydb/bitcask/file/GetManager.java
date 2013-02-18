/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file;

import it.holiday69.tinydb.bitcask.file.keydir.vo.KeyRecord;
import java.io.FileInputStream;

/**
 *
 * @author Stefano
 */
public class GetManager {
  
  //private final static int MAX_CONCURRENT_FIS = 20;
  
  //private final Map<File, FileInputStream> _openFisMap = new HashMap<File, FileInputStream>();
  
  public GetManager() {
    
  }
  
  public byte[] retrieveRecord(KeyRecord keyRecord) {
    
    try {
      
      FileInputStream fis = new FileInputStream(keyRecord.file);
      
      byte[] ret = new byte[(int)keyRecord.valueSize];
      
      fis.read(ret, (int)keyRecord.valuePosition, (int)keyRecord.valueSize);
      
      fis.close();
      
      return ret;
    } catch(Throwable th) {
      throw new RuntimeException("Unable to retrieve record from file: " + keyRecord.file + " : ", th);
    }
  }
  
}



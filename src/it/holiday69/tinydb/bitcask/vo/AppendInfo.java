/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.vo;

import java.io.File;

/**
 *
 * @author Stefano
 */
public class AppendInfo {
  
    public File appendFile;
    public int keySize;
    public int valueSize;
    public long valuePosition;
    public long timestamp;

    @Override
    public String toString() {
      return "AppendInfo{" + "appendFile=" + appendFile + ", keySize=" + keySize + ", valueSize=" + valueSize + ", valuePosition=" + valuePosition + ", timestamp=" + timestamp + '}';
    }
    
  }

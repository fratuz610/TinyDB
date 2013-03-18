/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.bitcask.file.vo;

import java.io.File;

/**
 *
 * @author Stefano
 */
public class KeyRecord {
  
  public File file;
  public int valueSize;
  public long valuePosition;
  public long timestamp;
  
  public KeyRecord withFile(File src) { this.file = src; return this; }

  public KeyRecord withValueSize(int src) { this.valueSize = src; return this; }
  public KeyRecord withValuePosition(long src) { this.valuePosition = src; return this; }
  public KeyRecord withTimestamp(long src) { this.timestamp = src; return this; }
  
  @Override
  public String toString() {
    return "KeyRecord{" + "file=" + file + ", valueSize=" + valueSize + ", valuePosition=" + valuePosition + ", timestamp=" + timestamp + '}';
  }
}

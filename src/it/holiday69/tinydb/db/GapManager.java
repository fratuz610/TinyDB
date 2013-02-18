/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.tinydb.db.entity.Gap;
import it.holiday69.tinydb.db.entity.RecordRef;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class GapManager {
  
  private final static Logger log = Logger.getAnonymousLogger();
  
  private final int _gapSize = 10;
  
  private RandomAccessFile _gapFile;
  
  private List<Long> _availableGapList = new LinkedList<Long>();
          
  private final ReentrantLock _gapFileLock = new ReentrantLock();
  
  public GapManager(File dbFolder, String dbName) {
    
    if(!dbFolder.exists())
      if(!dbFolder.mkdir())
        throw new RuntimeException("The database folder: " + dbFolder.getAbsolutePath() + " doesn't exist and cannot be created");
    
    try {
      _gapFile = new RandomAccessFile(getGapFile(dbFolder, dbName), "rw");
      
      _gapFile.seek(0);
      
      while(true) {
        
        Gap gap = readGap();
        
        if(gap == null)
          break;
        
        if(gap.deleted)
          _availableGapList.add(_gapFile.getFilePointer() - _gapSize);
      }
    } catch(Throwable th) {
      throw new RuntimeException("Unable to open/create gap file: " + getGapFile(dbFolder, dbName) + " because: " + th.getMessage());
    }
    
  }
  
  private File getGapFile(File dbFolder, String dbName) {
    return new File(dbFolder, dbName+".gap");
  }
  
  public RecordRef acquireGap(int minSize) {
    
    _gapFileLock.lock();
    RecordRef ret = new RecordRef();
    
    try {
      
      _gapFile.seek(0);
      
      while(true) {
        
        Gap gap = readGap();
        
        if(gap.deleted)
          continue;
        
        if(gap.size >= minSize) {
          markAsAvailable(gap);
          ret.offset = gap.offset;
          ret.size = gap.size;
          
          Gap remainderGap =  new Gap();
          remainderGap.offset = gap.offset + minSize;
          remainderGap.size = gap.size - minSize;
          
          if(remainderGap.size > 0) {
            System.out.println("There is a reminder for " + remainderGap.size + " adding it");
            addGap(remainderGap);
          }
            
          break;
        }
      }
      
    } catch(IOException ex) {
      throw new RuntimeException("Unable to read gap file because: " + ex.getMessage());
    } finally {
      _gapFileLock.unlock();
    }
    
    return ret;
  }
    
  public void clear() {
    
    _gapFileLock.lock();
    try {
      _gapFile.setLength(0);
      _availableGapList.clear();
    } catch(Throwable th) {
      throw new RuntimeException("Unable to truncate gap file because: " + th.getMessage());
    } finally {
      _gapFileLock.unlock();
    }
  }
   
  private Gap readGap() {
    
    try {
      
      if(_gapFile.getFilePointer() == _gapFile.length())
        return null;
      
      byte[] gapBytes = new byte[_gapSize];
      _gapFile.read(gapBytes);
      
      return Gap.fromByteArray(gapBytes);
      
    } catch(IOException ex) {
      throw new RuntimeException("Unable to read GAP object because: " + ex.getMessage());
    }
  }
  
  public void addGap(RecordRef recordRef) {
    addGap(Gap.fromRecordRef(recordRef));
  }
  
  private void addGap(Gap newGap) {
    
    try {
      
      if(!_availableGapList.isEmpty()) {
          
        _gapFile.seek(_availableGapList.remove(0));
        
      } else {
        _gapFile.seek(_gapFile.length());
        
      }
      System.out.println("Adding gap at " + _gapFile.getFilePointer());
      
      _gapFile.write(Gap.toByteArray(newGap));
      
    } catch(IOException ex) {
      throw new RuntimeException("Unable to add a new GAP object because: " + ex.getMessage());
    }
  }
    
  private void seekBack() {
    try {
      
      if((_gapFile.getFilePointer() - _gapSize) < 0)
        return;
      
      System.out.println("Seeking back to: " + (_gapFile.getFilePointer()-_gapSize));
      
      _gapFile.seek(_gapFile.getFilePointer()-_gapSize);
      
    } catch(IOException ex) {
      throw new RuntimeException("Unable to seek back on the gap file because: " + ex.getMessage());
    }
  }
  
  private void markAsAvailable(Gap gap) {
    
    try {
      seekBack();
      _availableGapList.add(_gapFile.getFilePointer());
      gap.deleted = true;
      _gapFile.write(Gap.toByteArray(gap));
    } catch(IOException ex) {
      throw new RuntimeException("Unable mark gap as available because: " + ex.getMessage());
    }
    
  }
  
  public List<Gap> readAllGaps() {
    
    List<Gap> retList = new LinkedList<Gap>();
    
    try {
      
      _gapFile.seek(0);
      
      while(true) {
        
        Gap gap = readGap();
        
        if(gap == null)
          break;
        
        retList.add(gap);
      }
      
      return retList;
    } catch(IOException ex) {
      throw new RuntimeException("Unable to read all gaps because: "  + ex.getMessage());
    }
  }
  
}

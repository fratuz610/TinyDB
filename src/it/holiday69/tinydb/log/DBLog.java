/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.log;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author fratuz610
 */
public class DBLog {
  
  public enum LogLevel { FINER, FINE, INFO, WARNING, SEVERE }
  
  private static final String LOG_FILE_NAME = "tinydb.log";
  
  private static FileOutputStream _logFileOS;
  private static LogWorker _logWorker;
  private static Thread _logWorkerThread;
  private static LogLevel _logLevel;
  
  private static final LinkedBlockingQueue<String> _workingQueue = new LinkedBlockingQueue<String>();
  
  public static void start(File logFolder, LogLevel minLogLevel) {
    
    _logLevel = minLogLevel;
    
    if(_logFileOS != null)
      throw new RuntimeException("The log folder ca be set only once");
    
    if(!logFolder.exists())
      if(!logFolder.mkdir())
        throw new RuntimeException("The log folder " + logFolder + " does not exist and it cannot be created!");
    
    File logFile = new File(logFolder, LOG_FILE_NAME);
    
    if(logFile.exists())
      if(!logFile.delete())
        throw new RuntimeException("The log file " + logFile + " cannot be overwritten");
    
    try {
      _logFileOS = new FileOutputStream(logFile);
    } catch(Throwable th) {
      throw new RuntimeException("Unable to write to the log file " + _logFileOS, th);
    }
    
    _logWorker = new LogWorker(_logFileOS);
    _logWorkerThread = new Thread(_logWorker);
    _logWorkerThread.start();
  }
  
  public static DBLog getInstance(String name) {
    return new DBLog(name);
  }
  
  public String _logName;
  
  public DBLog(String logName) {
    _logName = logName;
  }
  
  public void fine(String message) {
    if(_logLevel.ordinal() <= LogLevel.FINE.ordinal())
      _workingQueue.add(formatTimestamp() + " FINE: " + _logName + ": " + message + "\n");
  }
  
  public void info(String message) {
    if(_logLevel.ordinal() <= LogLevel.INFO.ordinal())
      _workingQueue.add(formatTimestamp() + " INFO: " + _logName + ": " + message + "\n");
  }
  
  public void warning(String message) {
    if(_logLevel.ordinal() <= LogLevel.WARNING.ordinal())
      _workingQueue.add(formatTimestamp() + " WARNING: " + _logName + ": " + message + "\n");
  }
  
  public void severe(String message) {
    if(_logLevel.ordinal() <= LogLevel.SEVERE.ordinal())
      _workingQueue.add(formatTimestamp() + " SEVERE: " + _logName + ": " + message + "\n");
  }
  
  public static class LogWorker implements Runnable {
    
    private final FileOutputStream _fos;
    
    public LogWorker(FileOutputStream fos) {
      _fos = fos;
    }
    
    @Override
    public void run() {
      
      String logMessage = null;
      try {
      
        while(true) {

          if(Thread.interrupted())
            break;
          
          try {
            logMessage = _workingQueue.poll(10, TimeUnit.SECONDS);
          } catch(InterruptedException ex) {
            break;
          }

          if(logMessage != null) {
            _fos.write(logMessage.getBytes());
          }
          
        }
        
        _fos.close();
      } catch(Throwable th) {

      }
      
    }
  }
  
  public static void shutdown() {
    _logWorkerThread.interrupt();
    try {
      _logWorkerThread.join();
      _logFileOS.close();
    } catch(Throwable ex) { }
    
  }
  
  private static String formatTimestamp() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return formatter.format(new Date());
  }
}

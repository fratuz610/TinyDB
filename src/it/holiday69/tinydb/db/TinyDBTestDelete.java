/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.dataservice.query.OrderType;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.db.annotations.Id;
import it.holiday69.tinydb.db.annotations.Indexed;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class TinyDBTestDelete {
  
  public static class Message {
    @Id public Long messageId;
    @Indexed public String author;
    public String message;
    @Indexed public long timestamp = new Date().getTime();
    
    @Override
    public String toString() {
      return "Message{" + "messageId=" + messageId + ", author=" + author + ", message=" + message + ", timestamp=" + timestamp + '}';
    }
  }
  
  private static final Logger _log = Logger.getLogger(TinyDBTestDelete.class.getSimpleName());
  
  public static void main(String[] args) throws IOException {
    
    FileInputStream configFile = new FileInputStream("logging.properties");
    LogManager.getLogManager().readConfiguration(configFile);
        
    TinyDBDataService dataService = new TinyDBDataService(new BitcaskOptions()
            .withCompactEvery(10, TimeUnit.MINUTES)
            .withRecordPerFile(5000)
            .withAsyncPuts(false)
            .withCacheSize(8*1024*1024), new ScheduledThreadPoolExecutor(2));
    
    _log.info("Inserting message: ");
    
    Message mess = new Message();
    mess.author = "Hello world";
    mess.message = "Message very nice " + Math.random()*new Date().getTime();
    dataService.put(mess);
    _log.info("mess: " + mess);
    
    Message retrievedMess = dataService.get("author", "Hello world", Message.class);
    
    _log.info("Retrieved mess: " + retrievedMess);
    
    dataService.delete(mess);
    
    retrievedMess = dataService.get("author", "Hello world", Message.class);
    
    _log.info("Retrieved mess after deleting: " + retrievedMess);
    
    dataService.shutdown(true);
    
  }
  
  private static String getMemoryUsage() {
    Runtime runtime = Runtime.getRuntime();

    NumberFormat format = NumberFormat.getInstance();

    StringBuilder sb = new StringBuilder();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    sb.append("memory in use: " + format.format((allocatedMemory - freeMemory) / 1024) + " / " + format.format(allocatedMemory / 1024));
    
    return sb.toString();
  }
}

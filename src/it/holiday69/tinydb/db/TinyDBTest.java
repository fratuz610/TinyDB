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

package it.holiday69.tinydb.db;

import it.holiday69.dataservice.query.OrderType;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.db.annotations.Id;
import it.holiday69.tinydb.db.annotations.Indexed;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class TinyDBTest {
  
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
  
  private static final Logger _log = Logger.getLogger(TinyDBTest.class.getSimpleName());
  
  public static void main(String[] args) throws IOException {
    
    FileInputStream configFile = new FileInputStream("logging.properties");
    LogManager.getLogManager().readConfiguration(configFile);
    
    TinyDBDataService dataService = new TinyDBDataService(new TinyDBOptions()
            .withCompactEvery(10, TimeUnit.MINUTES)
            .withRecordPerFile(5000)
            .withCacheSize(8*1024*1024)
            .withExecutoPoolSize(5));
        
    dataService.mapClass(Message.class, 10);
    
    long start;
    long end;
    
    List<Message> deleteMessageList = new LinkedList<Message>();
    
    start = new Date().getTime();
    _log.info("Inserting messages: ");
    for(int i = 0; i < 20000; i++) {
      Message mess = new Message();
      mess.author = "Myself " + Math.random()*new Date().getTime();
      mess.message = "Message very nice " + Math.random()*new Date().getTime();
      dataService.put(mess);
      deleteMessageList.add(mess);
    }
    end = new Date().getTime();
    _log.info("Insertion complete! operation took: " + (end - start) + " millis");
    
    
    Message mess = new Message();
    mess.author = "Myself " + Math.random()*new Date().getTime();
    mess.message = "Message very nice " + Math.random()*new Date().getTime();
    dataService.put(mess);
    
    start = new Date().getTime();
    dataService.get(100l, Message.class);
    end = new Date().getTime();
    _log.info("First operation took: " + (end - start) + " millis");
    
    _log.info("Retrieving all messages");
    
    start = new Date().getTime();
    List<Message> messList = dataService.getList(new Query()
            .filter("timestamp >", 1l)
            .orderBy("timestamp", OrderType.DESCENDING)
            .limit(1), Message.class);
    
    _log.info("Message list size: " + messList.size());
    end = new Date().getTime();
    _log.info("All messages retrieved! operation took: " + (end - start) + " millis");
    
    for(int i = 2; i < 10; i++) {
      _log.info("Retrieving all messages (x" + i + ")");

      start = new Date().getTime();
      messList = dataService.getList(new Query()
              .filter("timestamp >", 1l)
              .orderBy("timestamp")
              .limit(100), Message.class);

      _log.info("Message list size: " + messList.size());
      end = new Date().getTime();
      _log.info("All messages retrieved! operation took: " + (end - start) + " millis");
      
      Message oneMess = messList.get((int)(30*Math.random()));
      
      start = new Date().getTime();
      oneMess = dataService.get(oneMess.messageId, Message.class);
      end = new Date().getTime();
      _log.info("Simple Get on the primary key: operation took: " + (end - start) + " millis");
      
      start = new Date().getTime();
      oneMess = dataService.get("timestamp", oneMess.timestamp, Message.class);
      end = new Date().getTime();
      _log.info("Simple Get on the a field key: operation took: " + (end - start) + " millis");
      
      _log.info("Memory usage after this iteration: " + getMemoryUsage());
    }
    
    _log.info("Number of objects stored: " + dataService.getResultSetSize(Message.class));
    
    _log.info("About to delete some messages");
    dataService.deleteAll(deleteMessageList);
    
    _log.info("Number of objects stored after deleting: " + dataService.getResultSetSize(Message.class));
    
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

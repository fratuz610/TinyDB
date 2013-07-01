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
public class TinyDBSmallTest {
  
  public static class Message {
    @Id public Long messageId;
    @Indexed public String author;
    public String message;
    @Indexed public long timestamp = new Date().getTime();
    @Indexed public List<String> tagList = new LinkedList<String>();

    @Override
    public String toString() {
      return "Message{" + "messageId=" + messageId + ", author=" + author + ", message=" + message + ", timestamp=" + timestamp + ", tagList=" /*+ tagList*/ + '}';
    }
    
  }
  
  private static final Logger _log = Logger.getLogger(TinyDBSmallTest.class.getSimpleName());
  
  public static void main(String[] args) throws IOException {
    
    FileInputStream configFile = new FileInputStream("verboselogging.properties");
    LogManager.getLogManager().readConfiguration(configFile);
    
    TinyDBDataService dataService = new TinyDBDataService(new TinyDBOptions()
            .withCompactEvery(10, TimeUnit.MINUTES)
            .withRecordPerFile(5000)
            .withCacheSize(8*1024*1024)
            .withExecutorPoolSize(5)
            .withAsyncUpdates(false));
        
    dataService.mapClass(Message.class, 10);
    
    long start;
    long end;
    
    List<Message> deleteMessageList = new LinkedList<Message>();
    
    start = new Date().getTime();
    _log.info("Inserting messages: ");
    for(int i = 0; i < 200; i++) {
      Message mess = new Message();
      mess.author = "Myself " + Math.random()*new Date().getTime();
      mess.message = "Message very nice " + Math.random()*new Date().getTime();
      mess.tagList.add("tag1");
      mess.tagList.add("tag2");
      mess.tagList.add("tag3");
      mess.tagList.add("tag4");
      mess.tagList.add("tag5");
      mess.tagList.add("tag6");
      dataService.put(mess);
      deleteMessageList.add(mess);
    }
    end = new Date().getTime();
    _log.info("Insertion complete! operation took: " + (end - start) + " millis");
    
    
    
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

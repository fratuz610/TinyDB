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

import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.db.annotations.Id;
import it.holiday69.tinydb.db.annotations.Indexed;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
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
public class TinyDBTestIn {
  
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
  
  private static final Logger _log = Logger.getLogger(TinyDBTestIn.class.getSimpleName());
  
  public static void main(String[] args) throws IOException {
    
    FileInputStream configFile = new FileInputStream("logging.properties");
    LogManager.getLogManager().readConfiguration(configFile);
    
    TinyDBDataService dataService = new TinyDBDataService(new TinyDBOptions()
            .withCompactEvery(10, TimeUnit.MINUTES)
            .withRecordPerFile(5000)
            .withCacheSize(8*1024*1024)
            .withExecutorPoolSize(5)
            .withAsyncUpdates(false)
            .withDbFolder(System.getProperty("user.home") + "/tinydb-test-in"));
    
    _log.info("Using data folder: '" + System.getProperty("user.home") + "/tinydb-test-in'");
        
    dataService.mapClass(Message.class, 10);
    
    long start;
    long end;
    
    List<Message> deleteMessageList = new LinkedList<Message>();
    
    int tagOneOrTwoCnt = 0;
    int tagThreeOrFourCnt = 0;
    
    start = new Date().getTime();
    _log.info("Inserting messages: ");
    for(int i = 0; i < 2000; i++) {
      Message mess = new Message();
      mess.author = "Myself " + Math.random()*new Date().getTime();
      mess.message = "Message very nice " + Math.random()*new Date().getTime();
      
      if(Math.random() >= 0.5) {
        mess.tagList.add("tag1");
        mess.tagList.add("tag2");
        tagOneOrTwoCnt++;
      } else {
        mess.tagList.add("tag3");
        mess.tagList.add("tag4");
        tagThreeOrFourCnt++;
      }
      
      dataService.put(mess);
      deleteMessageList.add(mess);
    }
    end = new Date().getTime();
    _log.info("Insertion complete! operation took: " + (end - start) + " millis");
    
    List<Message> tagOneOrTwoList = dataService.getList(new Query().filter("tagList in", Arrays.asList("tag1", "tag2")), Message.class);
    List<Message> tagThreeOrFourList = dataService.getList(new Query().filter("tagList in", Arrays.asList("tag3", "tag4")), Message.class);
    List<Message> tagOneOrThreeList = dataService.getList(new Query().filter("tagList in", Arrays.asList("tag1", "tag3")), Message.class);
    
    _log.info("tagOneOrTwoList: " + tagOneOrTwoList.size() + " elements: expected: " + tagOneOrTwoCnt);
    _log.info("tagThreeOrFourList: " + tagThreeOrFourList.size() + " elements, expected: " + tagThreeOrFourCnt);
    _log.info("tagOneOrThreeList: " + tagOneOrThreeList.size() + " elements, expected: 2000");
    
    dataService.shutdown(true);
    
  }
  
}

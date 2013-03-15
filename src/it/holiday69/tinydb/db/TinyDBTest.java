/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.dataservice.DataService;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.db.annotations.Id;
import it.holiday69.tinydb.db.annotations.Indexed;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Stefano
 */
public class TinyDBTest {
  
  public static class Message {
    @Id public Long messageId;
    public String author;
    public String message;
    @Indexed public long timestamp = new Date().getTime();

    @Override
    public String toString() {
      return "Message{" + "messageId=" + messageId + ", author=" + author + ", message=" + message + ", timestamp=" + timestamp + '}';
    }
  }
  
  public static void main(String[] args) {
    
    DataService dataService = new TinyDBDataService(new BitcaskOptions()
            .withCompactEvery(25, TimeUnit.SECONDS)
            .withRecordPerFile(15));
    
    for(int i = 0; i < 200; i++) {
      Message mess = new Message();
      mess.author = "Myself " + Math.random()*new Date().getTime();
      mess.message = "Message very nice " + Math.random()*new Date().getTime();
      dataService.put(mess);
    }
    
    List<Message> messList = dataService.getList(Message.class);
    
    for(Message mess : messList) {
      System.out.println("Message: " + mess);
    }
    
  }
}

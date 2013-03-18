/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.dataservice.DataService;
import it.holiday69.dataservice.query.OrderType;
import it.holiday69.dataservice.query.Query;
import it.holiday69.tinydb.bitcask.BitcaskOptions;
import it.holiday69.tinydb.db.annotations.Id;
import it.holiday69.tinydb.db.annotations.Indexed;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
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
    public String author;
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
        
    TinyDBDataService dataService = new TinyDBDataService(new BitcaskOptions()
            .withCompactEvery(25, TimeUnit.SECONDS)
            .withRecordPerFile(15));
    
    /*
    _log.info("Inserting messages: ");
    for(int i = 0; i < 20; i++) {
      Message mess = new Message();
      mess.author = "Myself " + Math.random()*new Date().getTime();
      mess.message = "Message very nice " + Math.random()*new Date().getTime();
      dataService.put(mess);
    }
    _log.info("Insertion complete! retrieving messages");
    */
    List<Message> messList = dataService.getList(new Query()
            .filter("timestamp >=", 1363563918223l)
            .orderBy("timestamp", OrderType.DESCENDING), Message.class);
    
    _log.info("Message list size: " + messList.size());
    
    for(Message mess : messList) {
      _log.info("Message: " + mess);
    }
    
    dataService.shutdown(true);
    
  }
}

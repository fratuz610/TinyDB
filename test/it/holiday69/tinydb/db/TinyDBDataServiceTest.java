/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db;

import it.holiday69.tinydb.db.annotations.Id;
import it.holiday69.tinydb.db.annotations.Indexed;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Stefano
 */
public class TinyDBDataServiceTest {
  
  public static class Message {
    
    @Id public Long messageId;
    @Indexed public String author;
    public String message;
    @Indexed public long timestamp = new Date().getTime();
    
    @Override
    public String toString() {
      return "Message{" + "messageId=" + messageId + ", author=" + author + ", message=" + message + ", timestamp=" + timestamp + '}';
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 37 * hash + (this.messageId != null ? this.messageId.hashCode() : 0);
      hash = 37 * hash + (this.author != null ? this.author.hashCode() : 0);
      hash = 37 * hash + (this.message != null ? this.message.hashCode() : 0);
      hash = 37 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Message other = (Message) obj;
      if (this.messageId != other.messageId && (this.messageId == null || !this.messageId.equals(other.messageId))) {
        return false;
      }
      if ((this.author == null) ? (other.author != null) : !this.author.equals(other.author)) {
        return false;
      }
      if ((this.message == null) ? (other.message != null) : !this.message.equals(other.message)) {
        return false;
      }
      if (this.timestamp != other.timestamp) {
        return false;
      }
      return true;
    }
    
    
  }
  
  private TinyDBDataService _dataService;
  
  public TinyDBDataServiceTest() {
  }
  
  @Before
  public void setUp() {
    
    _dataService = new TinyDBDataService(new TinyDBOptions()
            .withDbFolder(System.getProperty("java.io.tmpdir") + "\\tinydb-test")
            .withAsyncUpdates(false)
            .withCompactEvery(10, TimeUnit.MINUTES)
            .withRecordPerFile(5000)
            .withCacheSize(8*1024*1024)
            .withExecutorPoolSize(5));
        
    _dataService.mapClass(TinyDBTest.Message.class, 10);
    
  }
  
  @After
  public void tearDown() {
    _dataService.shutdown(false);
  }

  /**
   * Basic testing for the TinyDBDataService
   */
  @Test
  public void testDBDataService() {
    
    Message mess = new Message();
    
    mess.author = "author";
    
    // PUT
    _dataService.put(mess);
    
    System.out.println("Got message: " + mess);
    
    // GET
    Message retMess = _dataService.get(mess.messageId, Message.class);
    
    assertEquals(mess, retMess);
    
    // DELETE
    _dataService.delete(mess);
    
    Message deleteMess = _dataService.get(mess.messageId, Message.class);
    
    assertEquals(null, deleteMess);
  }

}

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

package it.holiday69.tinydb.bitcask;

import it.holiday69.tinydb.bitcask.vo.Key;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 *
 * @author Stefano
 */
public class BitcaskTest {
  
  private final static Logger _log = Logger.getLogger(BitcaskTest.class.getSimpleName());
  
  public static class Message {
    public String author = "myname-" + new Date().getTime();
    public String message = "new-message-" + new Date().getTime();

    @Override
    public String toString() {
      return "Message{" + "author=" + author + ", message=" + message + '}';
    }
    
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    
    FileInputStream configFile = new FileInputStream("logging.properties");
    LogManager.getLogManager().readConfiguration(configFile);
    
    Bitcask bitcask = new Bitcask("hello-world", new BitcaskOptions()
            .withCompactEvery(25, TimeUnit.SECONDS)
            .withRecordPerFile(10));
    
    String savedAuth = null;
    
    while(true) {
    
      _log.info("New batch add...");
      
      for(int i = 0; i < 25; i++) {

        Message mess = new Message();

        if(savedAuth == null)
          savedAuth = mess.author;

        bitcask.put(new Key().fromString(mess.author), mess);
     }

      Message retMess = (Message) bitcask.get(new Key().fromString(savedAuth));

    _log.info("retMess: '" + retMess + "' waiting 30 secs");

    Thread.sleep(30000);
    }
    
  }
}

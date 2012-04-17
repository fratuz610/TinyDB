/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.test;

import it.holiday69.tinydb.DataService;
import it.holiday69.tinydb.jdbm.TinyDBDataService;
import it.holiday69.tinydb.query.OrderType;
import it.holiday69.tinydb.query.Query;
import it.holiday69.tinydb.test.entity.DBMessage;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author fratuz610
 */
public class TinyDBTest {

  private static final DataService _dataService = new TinyDBDataService();
  
  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
        
    //addStuff();
    //readStuff();
    readSpecificStuff();
    
    /*
    DBMessage mess = new DBMessage();
    //mess.id = 100;
    mess.author = "author";
    mess.message = "mess";
    
    _dataService.put(mess);
    
    List<DBMessage> messageList = _dataService.getList(DBMessage.class);
    
    for(DBMessage mess : messageList)
      System.out.println("Message: " + mess.toString());
    
    /*
    DBMessage mess = _dataService.get("hello", DBMessage.class);
    
    System.out.println("Mess : " + mess.author);
    */
  }
  
  public static void addStuff() {
    
    List<DBMessage> messList = new LinkedList<DBMessage>();
    for(int i = 0; i < 500; i++)
      messList.add(new DBMessage().withAuthor("auth: " + (int) (Math.random()*20) ).withNumber((int) (Math.random()*20)));
    
    _dataService.putAll(messList);
  }
  
  public static void readStuff() {
    
   List<DBMessage> messageList = _dataService.getList(DBMessage.class);
    
   for(DBMessage mess : messageList)
     System.out.println("MEssage: " + mess.toString());
    
  }
  
  public static void readSpecificStuff() {
    
    
    //DBMessage mess = _dataService.get(99l, DBMessage.class);
    
    //DBMessage mess = _dataService.get("author", "auth: 6", DBMessage.class);
    
    //List<DBMessage> messList = _dataService.getList("number <=", 9, DBMessage.class);
    
    //List<DBMessage> messList = _dataService.getList(DBMessage.class);
    
    List<DBMessage> messList = _dataService.getList(new Query().filter("number >", 9).orderBy("date", OrderType.ASCENDING), DBMessage.class);
        
    System.out.println("returned set size: " + messList.size());
    for(DBMessage mess : messList)
      System.out.println("mess: " + mess.toString());
  }
}

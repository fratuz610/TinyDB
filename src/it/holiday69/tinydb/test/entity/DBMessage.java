package it.holiday69.tinydb.test.entity;

import it.holiday69.tinydb.jdbm.annotations.Id;
import it.holiday69.tinydb.jdbm.annotations.Indexed;
import java.io.Serializable;
import java.util.Date;

public class DBMessage implements Serializable {

  @Id public long id;
  
  @Indexed public Date date = new Date();
  @Indexed public String author;
  public String message;
  @Indexed public int number;
  
  @Override
  public String toString() {
    return "[DBMessage id: '" + id + "' author: '" + author + "' message: '" + message + "' number: '"+number+"' date: "+date.getTime()+"]";
  }
  
  public DBMessage withAuthor(String author) { this.author = author; return this; }
  public DBMessage withMessage(String message) { this.message = message; return this; }
  public DBMessage withNumber(int number) { this.number = number; return this; }
}

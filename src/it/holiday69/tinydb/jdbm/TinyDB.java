/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.jdbm;

import it.holiday69.tinydb.jdbm.exception.TinyDBException;
import java.io.File;
import java.util.logging.Logger;
import org.apache.jdbm.DB;
import org.apache.jdbm.DBMaker;

/**
 *
 * @author fratuz610
 */
public class TinyDB {
  
  private final static Logger log = Logger.getLogger(TinyDB.class.getSimpleName());
  
  private final static Object _dbMutex = new Object();
  
  private static String _dbFolder = "db";
  private static String _dbName = "db";
  private static DB _db;
  
  /**
   * Get / returns an instance of the database handle
   * 
   * @return an instance of the database handle
   */
  public static DB getInstance() {
    
    synchronized(_dbMutex) {
      if(_db == null) {

        // the instance hasn't been created

        File dbFolder = new File(_dbFolder);
        if(!dbFolder.exists())
          dbFolder.mkdir();

        if(!dbFolder.exists() || !dbFolder.isDirectory())
          throw new TinyDBException("Unable to create the db hosting folder: '" + dbFolder + "'");

        DBMaker dbMaker = DBMaker.openFile(_dbFolder + "/" + _dbName);
        //dbMaker.disableCache();
        dbMaker.useRandomAccessFile();
        _db = dbMaker.make();
      }

      return _db;
    }
    
  }
  /**
   * Sets the name of the database.
   * 
   * The name is used to build the file data structure on disk
   * 
   * @param name The name to use for the database (default: "db")
   * @param folder The folder that will host the database root (default: current working folder)
   */
  public static void setDBName(String folder, String name) {
    
    synchronized(_dbMutex) {
      
      if(_db != null)
        throw new TinyDBException("Unable to modify the database name once an instance of the database has been created");
      
      _dbFolder = folder;
      _dbName = name;
    }
    
  }
  
}

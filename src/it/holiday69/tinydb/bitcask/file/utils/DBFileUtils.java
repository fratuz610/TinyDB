/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.holiday69.tinydb.bitcask.file.utils;

import it.holiday69.tinydb.db.utils.HashUtils;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.DatatypeConverter;

/**
 *
 * @author Stefano Fratini <stefano.fratini@yeahpoint.com>
 */
public class DBFileUtils {
  
  private final static Logger _log = Logger.getLogger(DBFileUtils.class.getSimpleName());

  public static File getDBFile(File dbFolder, String dbName, int index) {
    return new File(dbFolder, getTranslatedDBName(dbName) + ".db." + index);
  }
  
  public static File getHintFile(File dbFolder, String dbName) {
    return new File(dbFolder, getTranslatedDBName(dbName) + ".db.hint");
  }
  
  public static File getTempHintFile(File dbFolder, String dbName) {
    return new File(dbFolder, getTranslatedDBName(dbName) + ".db.hint.temp");
  }
  
  public static Integer getDBFileNumber(File dbFile) {
    Pattern pattern = Pattern.compile("(.*)\\.db\\.(\\d+)$");
    Matcher matcher = pattern.matcher(dbFile.getName());
    
    if(!matcher.matches())
      return null;
    
    return Integer.parseInt(matcher.group(2));
  }
  
  public static boolean isValidDBFileName(String fileName, String dbName) {
    Pattern pattern = Pattern.compile("(.*)\\.db\\.(\\d+)$");
    Matcher matcher = pattern.matcher(fileName);
    return matcher.matches() && matcher.group(1).equals(getTranslatedDBName(dbName));
  }
  
  public static File[] getDBFileList(File dbFolder, final String dbName) {
    
    File[] dbFileList = dbFolder.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String fileName) {
        return isValidDBFileName(fileName, dbName);
      }
    });
    
    // we sort the array in order of lastModified
    Arrays.sort(dbFileList, new Comparator<File>() {

      @Override
      public int compare(File o1, File o2) {
        
        // 2 files with the same name => they are the same
        if(o1.getAbsolutePath().equals(o2.getAbsolutePath()))
          return 0;
        
        if(o1.lastModified() < o2.lastModified()) 
          return -1;
        else if(o1.lastModified() > o2.lastModified())
          return +1;
        else
          return 0;
      }
    });
    
    return dbFileList;
  }
  
  private static String getTranslatedDBName(String dbName) {
    return DatatypeConverter.printHexBinary(HashUtils.SHA256(dbName)).substring(0, 20);
  }
}

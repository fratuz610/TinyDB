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

package it.holiday69.tinydb.bitcask.file.utils;

import it.holiday69.tinydb.utils.HashUtils;
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

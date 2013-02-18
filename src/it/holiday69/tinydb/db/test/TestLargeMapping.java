/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author fratuz610
 */
public class TestLargeMapping {
  
  public static void main(String args[]) throws IOException {
    
    System.out.println("About to create a 2gb mapped file");
    RandomAccessFile raf = new RandomAccessFile(new File("test-large.data"), "rw");
    MappedByteBuffer buf = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1024*1024*1024);
    System.out.println("All done");
  }
  
}

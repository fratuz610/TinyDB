/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package it.holiday69.tinydb.db.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

/**
 *
 * @author Stefano Fratini <stefano.fratini@yeahpoint.com>
 */
public class HashUtils {

  // SHA256

  public static byte[] SHA256(String source) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      return sha.digest(source.getBytes());
    } catch (NoSuchAlgorithmException ex) {
      Logger.getLogger(HashUtils.class.getSimpleName()).severe("Unable to use SHA256 Encryption, returning null: " + ex.getMessage());
      return null;
    }
  }
  
  public static byte[] SHA1(String source) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-1");
      return sha.digest(source.getBytes());
    } catch (NoSuchAlgorithmException ex) {
      Logger.getLogger(HashUtils.class.getSimpleName()).severe("Unable to use SHA1 Encryption, returning null: " + ex.getMessage());
      return null;
    }
  }


}

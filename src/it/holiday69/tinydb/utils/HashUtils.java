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

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

import org.iq80.snappy.Snappy;

/**
 *
 * @author Stefano Fratini <mail@stefanofratini.it>
 */
public class SnappyHelper {
  
  public byte[] compress(byte[] source) {
    return Snappy.compress(source);
  }
  
  public byte[] uncompress(byte[] source) {
    return Snappy.uncompress(source, 0, source.length);
  }
  
}

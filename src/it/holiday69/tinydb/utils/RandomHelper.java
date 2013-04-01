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

package it.holiday69.tinydb.utils;

/**
 *
 * @author fratuz610
 */
public class RandomHelper {

	private final String RANDOM_DELIMETER_CHAR_LIST = "0123456789";
  private final int RANDOM_DELIMETER_LENGTH = 13;

  public String getRandomDelimiter() {

    StringBuilder delimeter = new StringBuilder();
    while(delimeter.length() < RANDOM_DELIMETER_LENGTH) {
      int randPos = (int) Math.floor(Math.random() * RANDOM_DELIMETER_CHAR_LIST.length());
      delimeter.append(RANDOM_DELIMETER_CHAR_LIST.charAt(randPos));
    }
    return delimeter.toString();
  }
}

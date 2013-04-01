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

package it.holiday69.tinydb.db.vo;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author fratuz610
 */
public class ClassInfo {
  
  public enum IDFieldType { STRING, LONG, DOUBLE }
  
  public String idFieldName;
  public IDFieldType idFieldType;
  public List<String> indexedFieldNameList = new LinkedList<String>();
}

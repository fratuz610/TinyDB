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

import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author fratuz610
 */
public class DebugHelper {

  private final static List<String> _debugList = Collections.synchronizedList(new LinkedList<String>());
	
	public void log(String logString) {
    
    GregorianCalendar cal = new GregorianCalendar();
    
    String stringDay = autopad(cal.get(Calendar.DATE), 2);
    String stringMonth = autopad(cal.get(Calendar.MONTH) + 1, 2);
    String stringYeah = "" + cal.get(Calendar.YEAR);
    String stringHour = autopad(cal.get(Calendar.HOUR_OF_DAY), 2);
    String stringMinutes = autopad(cal.get(Calendar.MINUTE), 2);
    String stringSeconds = autopad(cal.get(Calendar.SECOND), 2);
        
    _debugList.add(stringDay + "-" + stringMonth + "-" + stringYeah + " " + stringHour + ":" + stringMinutes + ":" + stringSeconds + " " + logString);
  }
  
  public void clear() {
    _debugList.clear();
  }
  
  public List<String> getLogList() {
    return new LinkedList<String>(_debugList);
  }
  
  private String autopad(int value, int numDigits) {
    
    String strVal = "" + value;
    while(strVal.length() < numDigits)
      strVal = "0" + strVal;
    
    return strVal;
  }
}

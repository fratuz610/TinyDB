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
 * @author Stefano Fratini <stefano.fratini@yeahpoint.com>
 */
public class ExceptionUtils {

  public static String formatStackTrace(StackTraceElement[] stackTraceList) {

    String[] stackStringList = new String[stackTraceList.length];

    int cnt = 0;
    for(StackTraceElement elem : stackTraceList)
      stackStringList[cnt] = "[" + cnt++ + "] " + elem.toString();

    return join("\n", stackStringList);
  }

  private static String join(String glue, String[] s)
  {
    int k=s.length;
    if (k==0)
      return null;
    StringBuilder out=new StringBuilder();
    out.append(s[0]);
    for (int x=1;x<k;++x)
      out.append(glue).append(s[x]);
    return out.toString();
  }
   
  public static String getFullExceptionInfo(Throwable e){

    String info = e.getClass().getSimpleName() + " : " + e.getMessage();
    
    if(e.getCause() != null)
      return info + " CAUSED BY: " + e.getCause().getClass().getSimpleName() + " : " + e.getCause().getMessage() + "\nStack trace:\n" + formatStackTrace(e.getCause().getStackTrace());
    else
      return e.getClass().getSimpleName() + " : " + e.getMessage() + "\nStack trace:\n" + formatStackTrace(e.getStackTrace());
  }
  
  public static String getStackTraceAsString(Throwable e){

    StackTraceElement[] list = e.getStackTrace();
    String[] stackStringList = new String[list.length];

    int cnt = 0;
    for(StackTraceElement elem : list)
      stackStringList[cnt] = "[" + cnt++ + "] " + elem.toString();
    
    return StringUtils.join("\n", stackStringList);
  }
  
  public static String getDisplableExceptionInfo(Throwable th) {
    
    if(th.getMessage() != null)
      return th.getMessage();
    
    if(th.getCause() != null && th.getCause().getMessage() != null)
      return th.getCause().getMessage();
    
    return th.getClass().getName();
  }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author fratuz610
 */
public class IOHelper {

	public final int DEFAULT_BUFFER_SIZE = 1024 * 4;
	
	public long copy(InputStream input, OutputStream output) throws IOException {
		return copy(input, output, null, null);
	}
  
  public long copy(InputStream input, OutputStream output, AtomicLong bytesRead, AtomicLong bytesPerSec) throws IOException {
    
    long copyStartTime = System.nanoTime();
    
    // resets counter
    if(bytesRead!=null) bytesRead.set(0);
    
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    long count = 0;
    int n = 0;
    while (-1 != (n = input.read(buffer))) {
        output.write(buffer, 0, n);
        output.flush();
        count += n;
        if(bytesRead!=null) bytesRead.set(count);
              
        long copyTime = System.nanoTime() - copyStartTime;
        long countNormalized = (long) (count * 1e9);
              
        if(bytesPerSec != null) bytesPerSec.set(countNormalized / copyTime);
    }
    return count;
  }
  
  public String readAsString(InputStream input) throws IOException {
    return new String(readAsByteArray(input));
	}
  
  public String readAsString(InputStream input, AtomicLong bytesRead, AtomicLong bytesPerSec) throws IOException {
    return new String(readAsByteArray(input, bytesRead, bytesPerSec));
	}
  
  public byte[] readAsByteArray(InputStream input) throws IOException {
    return readAsByteArray(input, null, null);
	}
  
  public byte[] readAsByteArray(InputStream input, AtomicLong bytesRead, AtomicLong bytesPerSec) throws IOException {
    
    long copyStartTime = System.nanoTime();
    
    if(bytesRead!=null) bytesRead.set(0);
    
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    
		byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    long count = 0;
    int n = 0;
    while (-1 != (n = input.read(buffer))) {
        output.write(buffer, 0, n);
        count += n;
        if(bytesRead!=null) bytesRead.set(count);
        
        long copyTime = System.nanoTime() - copyStartTime;
        long countNormalized = (long) (count * 1e9);
              
        if(bytesPerSec != null) bytesPerSec.set(countNormalized / copyTime);
    }
    
    return output.toByteArray();
  }
}

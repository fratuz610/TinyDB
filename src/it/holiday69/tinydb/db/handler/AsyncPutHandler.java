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

package it.holiday69.tinydb.db.handler;

import it.holiday69.tinydb.db.BitcaskManager;
import it.holiday69.tinydb.db.TinyDBMapper;
import it.holiday69.tinydb.log.DBLog;
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class AsyncPutHandler implements PutHandler<Object> {
  
  private final DBLog _log = DBLog.getInstance(AsyncPutHandler.class.getSimpleName());
  
  private final ExecutorService _putExecutor;
  private final LinkedBlockingQueue<Object> _workingQueue = new LinkedBlockingQueue<Object>();
  
  private final AtomicBoolean _isShutdown = new AtomicBoolean(false);
  private final SyncPutHandler _putHandler;
  
  public AsyncPutHandler(BitcaskManager manager, TinyDBMapper dbMapper, ExecutorService executor) {
    _putHandler = new SyncPutHandler(manager, dbMapper);
    _putExecutor = executor;
    synchronized(_putExecutor) {
      _putExecutor.submit(new PutWorker());
    }
  }
  
  @Override
  public <T> void put(T newObj) {
    _workingQueue.add(newObj);

  }
  
  @Override
  public void shutdown() {
    _isShutdown.set(true);
    _workingQueue.clear();
  }
  
  public class PutWorker implements Runnable {
    
    @Override
    public void run() {
      
      Object obj = null;
      try {
      
        while(true) {

          if(_isShutdown.get() || Thread.interrupted()) {
            _log.info("Shutdown triggered, shutting down");
            break;
          }
          
          _log.fine("AsyncPutHandler: Polling from working queue");
          
          try {
            obj = _workingQueue.poll(10, TimeUnit.SECONDS);
          } catch(InterruptedException ex) {
            _log.fine("Got Interrupted, closing down");
            break;
          }

          if(obj != null) {
            _log.fine("AsyncPutHandler: Persising object: " + obj.getClass().getSimpleName());
            _putHandler.put(obj);
          }
          
        }
        
      } catch(Throwable th) {
        _log.severe("Exception while writing to the DB: " + ExceptionUtils.getFullExceptionInfo(th));
      }
      
    }
  }
}

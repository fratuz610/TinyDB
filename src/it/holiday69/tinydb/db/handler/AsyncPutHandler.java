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
import it.holiday69.tinydb.utils.ExceptionUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 *
 * @author fratuz610
 */
public class AsyncPutHandler implements PutHandler<Object> {
  
  private final Logger _log = Logger.getLogger(AsyncPutHandler.class.getSimpleName());
  
  private final BitcaskManager _bitcaskManager;
  private final TinyDBMapper _dbMapper;
  private ExecutorService _putExecutor;
  private final LinkedBlockingQueue<Object> _workingQueue = new LinkedBlockingQueue<Object>();
  
  private boolean _shutdown = false;
   
  public AsyncPutHandler(BitcaskManager manager, TinyDBMapper dbMapper, ExecutorService executor) {
    _bitcaskManager = manager;
    _dbMapper = dbMapper;
    
    _putExecutor = executor;
    _putExecutor.submit(new PutWorker());
  }
  
  @Override
  public <T> void put(T newObj) {
    _workingQueue.add(newObj);

  }
  
  @Override
  public void shutdown() {
    _shutdown = true;
  }
  
  public class PutWorker implements Runnable {
    
    private final SyncPutHandler _putHandler;
    
    public PutWorker() {
      _putHandler = new SyncPutHandler(_bitcaskManager, _dbMapper);
    }

    @Override
    public void run() {
      
      Object obj = null;
      try {
      
        while(true) {

          if(_shutdown && _workingQueue.isEmpty()) {
            _log.info("Empty working queue, closing down");
            break;
          }
          
          //_log.info("AsyncPutHandler: Polling from working queue");
          
          try {
            obj = _workingQueue.poll(10, TimeUnit.SECONDS);
          } catch(InterruptedException ex) {
            _log.info("Got Interrupted, setting shutdown flag");
            _shutdown = true;
          }

          if(obj != null) {
            //_log.info("AsyncPutHandler: Persising object");
            _putHandler.put(obj);
          }
          
        }
        
      } catch(Throwable th) {
        _log.severe("Exception while writing to the DB: " + ExceptionUtils.getFullExceptionInfo(th));
      }
      
    }
  }
}

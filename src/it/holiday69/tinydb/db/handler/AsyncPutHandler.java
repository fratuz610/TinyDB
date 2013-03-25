/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
    _putExecutor.shutdown();
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
      
      Object obj;
      try {
      
        while(true) {

          if(_shutdown && _workingQueue.isEmpty()) {
            _log.info("Empty working queue, closing down");
            break;
          }
          
          obj = _workingQueue.poll(10, TimeUnit.SECONDS);
          
          if(obj != null)
            _putHandler.put(obj);
          
        }
        
      } catch(InterruptedException ex) {
        _log.info("Got Interrupted, shutting down async writer");
        return;
      } catch(Throwable th) {
        _log.severe("Exception while writing to the DB: " + ExceptionUtils.getFullExceptionInfo(th));
      }
    }
  }
}

/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;

/**
 * Instance cache which uses <code>SoftReference</code> or <code>WeakReference</code>
 * Items can be removed from cache by Garbage Collector if
 *
 * @author Jan Kotek
 */
public class CacheWeakSoftRef implements Engine {


    protected interface CacheItem{
        long getRecid();
        Object get();
    }

    protected static final class CacheWeakItem extends WeakReference implements CacheItem{

        final long recid;

        public CacheWeakItem(Object referent, ReferenceQueue q, long recid) {
            super(referent, q);
            this.recid = recid;
        }

        @Override
        public long getRecid() {
            return recid;
        }
    }

    protected static final class CacheSoftItem extends SoftReference implements CacheItem{

        final long recid;

        public CacheSoftItem(Object referent, ReferenceQueue q, long recid) {
            super(referent, q);
            this.recid = recid;
        }

        @Override
        public long getRecid() {
            return recid;
        }
    }

    protected ReferenceQueue<CacheItem> queue = new ReferenceQueue<CacheItem>();

    protected Thread queueThread = new Thread("JDBM GC collector"){
        @Override
		public void run(){
            runRefQueue();
        }
    };


    protected LongConcurrentHashMap<CacheItem> items = new LongConcurrentHashMap<CacheItem>();


    protected Engine engine;
    final protected boolean useWeakRef;

    public CacheWeakSoftRef(Engine engine, boolean useWeakRef){
        this.engine = engine;
        this.useWeakRef = useWeakRef;

        queueThread.setDaemon(true);
        queueThread.start();
    }


    /** Collects items from GC and removes them from cache */
    protected void runRefQueue(){
        try{
            final ReferenceQueue<CacheItem> queue = this.queue;
            final LongConcurrentHashMap<CacheItem> items = this.items;

            while(true){
                CacheItem item = (CacheItem) queue.remove();
                items.remove(item.getRecid(), item);
                if(Thread.interrupted()) return;
            }
        }catch(InterruptedException e){
            //this is expected, so just silently exit thread
        }
    }

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        long recid = engine.recordPut(value, serializer);
        items.put(recid, useWeakRef?
                new CacheWeakItem(value, queue, recid) :
                new CacheSoftItem(value, queue, recid));
        return recid;
    }

    @Override
    public <A> A recordGet(long recid, Serializer<A> serializer) {
        CacheItem item = items.get(recid);
        if(item!=null){
            Object o = item.get();
            if(o == null)
                items.remove(recid);
            else{
                return (A) o;
            }
        }

        Object value = engine.recordGet(recid, serializer);
        if(value!=null){
            items.put(recid, useWeakRef?
                    new CacheWeakItem(value, queue, recid) :
                    new CacheSoftItem(value, queue, recid));
        }
        return (A) value;
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        items.put(recid, useWeakRef?
                new CacheWeakItem(value, queue, recid) :
                new CacheSoftItem(value, queue, recid));
        engine.recordUpdate(recid, value, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        items.remove(recid);
        engine.recordDelete(recid);
    }


    @Override
    public void close() {
        engine = null;
        items = null;
        queue = null;
        queueThread.interrupt();
        queueThread = null;
    }

    @Override
    public void commit() {
        engine.commit();
    }

    @Override
    public void rollback() {
        items.clear();
        engine.rollback();
    }

    @Override
    public long serializerRecid() {
        return engine.serializerRecid();
    }

    @Override
    public long nameDirRecid() {
        return engine.nameDirRecid();
    }

    @Override
    public boolean isReadOnly() {
        return engine.isReadOnly();
    }

}

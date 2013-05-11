# TinyDB

A simple and lightweight object oriented embeddable NoSQL DB written in Java

## Features

* Inspired by the [Bitcask key-value datastore from Riak](http://docs.basho.com/riak/1.2.0/tutorials/choosing-a-backend/Bitcask/)
    * All keys are kept in memory
    * Redo logs only on disk
* Ultra low memory and disk footprint
    * no useless waste of space/memory!
* Autocompact after a programmable interval
    * Consolidate into one file
    * Easy backups, no disk space waste
* Builtin write-through cache
* Scales easily to millions of items per entity with just a few megs of RAM
* Designed for embedded systems in mind (ARM)

## TODO

* Automatic S3 Backups

## Usage

### 1 - Define a pojo

    public static class Message {
        @Id public Long messageId;
        @Indexed public String author;
        public String message;
        @Indexed public long timestamp = new Date().getTime();
    }


* A pojo **must** have
    * At least **one field** marked with the `@Id` annotation (primary key)
        * Only `String`, `Long`, `Integer`, `Float` and `Double` types are supported
    * Zero or more `@Indexed` fields
        * Same limitation as the `@Id` field: only `String`, `Long`, `Integer`, `Float` and `Double` types
* If a field hasn't been indexed, it can't be queried for


### 2 - Start the TinyDBDataService singleton

    TinyDBDataService dataService = new TinyDBDataService(new TinyDBOptions()
            .withCompactEvery(10, TimeUnit.MINUTES)
            .withRecordPerFile(5000)
            .withCacheSize(8*1024*1024)
            .withExecutorPoolSize(5));

### 3 - Use and share among your threads!

#### PUT
    
    Message mess = new Message();
    mess.author = "Myself " + Math.random()*new Date().getTime();
    mess.message = "Message very nice " + Math.random()*new Date().getTime();
    dataService.put(mess);

#### GET BY KEY

    Message m = dataService.get(100l, Message.class);
    
#### GETLIST BY QUERY
    
    List<Message> messList = dataService.getList(new Query()
            .filter("timestamp >", 1l)
            .orderBy("timestamp", OrderType.DESCENDING)
            .limit(1), Message.class);

#### DELETE

    dataService.delete(mess);
    
### 4 - Shutdown (if you like)

    dataService.shutdown(true); // to autocompact
    dataService.shutdown(false); // no autocompact
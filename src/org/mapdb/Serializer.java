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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.CRC32;

/**
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public interface Serializer<A> {

    /**
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out ObjectOutput to save object into
     * @param value Object to serialize
     */
    public void serialize(DataOutput out, A value)
            throws IOException;


    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param in to read serialized data from
     * @param available how many bytes are available in DataInput for reading, may be -1 (in streams) or 0 (null).
     * @return deserialized object
     * @throws java.io.IOException
     */
    public A deserialize(DataInput in, int available)
            throws IOException;

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    Serializer<String> STRING_SERIALIZER = new Serializer<String>() {

        @Override
		public void serialize(DataOutput out, String value) throws IOException {
            final byte[] bytes = value.getBytes(Utils.UTF8);
            out.write(bytes);
        }


        @Override
		public String deserialize(DataInput in, int available) throws IOException {
            byte[] bytes = new byte[available];
            in.readFully(bytes);
            return new String(bytes, Utils.UTF8);
        }
    };




    /** Serializes Long into 8 bytes, used mainly for testing.
     * Does not handle null values.*/
    Serializer<Long> LONG_SERIALIZER = new Serializer<Long>() {
        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            out.writeLong(value);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            return in.readLong();
        }
    };

    /** Serializes Integer into 4 bytes, used mainly for testing.
     * Does not handle null values.*/
    Serializer<Integer> INTEGER_SERIALIZER = new Serializer<Integer>() {
        @Override
        public void serialize(DataOutput out, Integer value) throws IOException {
            out.writeInt(value);
        }

        @Override
        public Integer deserialize(DataInput in, int available) throws IOException {
            return in.readInt();
        }
    };


    Serializer<byte[] > BYTE_ARRAY_SERIALIZER = new Serializer<byte[]>() {

        @Override
        public void serialize(DataOutput out, byte[] value) throws IOException {
            out.write(value);
        }

        @Override
        public byte[] deserialize(DataInput in, int available) throws IOException {
            byte[] ret = new byte[available];
            in.readFully(ret);
            return ret;
        }
    } ;


    /** always writes zero length data, and always deserializes it as null */
    Serializer<Object> NULL_SERIALIZER = new Serializer<Object>() {
        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            return null;
        }
    };

    /** basic serializer for most classes in 'java.lang' and 'java.util' packages*/
    @SuppressWarnings("unchecked")
    Serializer<Object> BASIC_SERIALIZER = new SerializerBase();

    /** Basic serializer for most classes in 'java.lang' and 'java.util' packages.
     * This serializer simulates CPU intensive (de)serialization by adding some calculations to slow down CPU.*/
    @SuppressWarnings("unchecked")
    Serializer<Object> SLOW_BASIC_SERIALIZER = new Serializer<Object>(){

        private void slowDown(){
            long result = 0;
            long t = System.currentTimeMillis();
            while(t==System.currentTimeMillis()){
                result += 1;
                if(result==Long.MIN_VALUE) result++;
            }
            if(result==Long.MIN_VALUE)
                throw new InternalError(); //this will never happen thanks to condition in loop;
        }

        @Override
        public void serialize(DataOutput out, Object value) throws IOException {
            slowDown();
            BASIC_SERIALIZER.serialize(out, value);
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            slowDown();
            return BASIC_SERIALIZER.deserialize(in, available);
        }
    };

    /**
     * Adds CRC32 checksum at end of each record to check data integrity.
     * It throws 'IOException("CRC32 does not match, data broken")' on de-serialization if data are corrupted
     */
    public static final Serializer<byte[]> CRC32_CHECKSUM = new Serializer<byte[]>() {
        @Override
        public void serialize(DataOutput out, byte[] value) throws IOException {
            if(value == null || value.length==0) return;
            CRC32 crc = new CRC32();
            crc.update(value);
            out.write(value);
            out.writeInt((int) crc.getValue());
        }

        @Override
        public byte[] deserialize(DataInput in, int available) throws IOException {
            if(available==0) return null;
            byte[] value = new byte[available-4];
            in.readFully(value);
            CRC32 crc = new CRC32();
            crc.update(value);
            int checksum = in.readInt();
            if(checksum!=(int)crc.getValue()){
                throw new IOException("CRC32 does not match, data broken");
            }
            return value;
        }
    };


}


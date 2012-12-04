/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.tinydb.db.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author fratuz610
 */
public class ByteBufferOutputStream extends OutputStream {
    
  ByteBuffer buf;

    public ByteBufferOutputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public synchronized void write(int b) throws IOException {
        buf.put((byte) b);
    }

    public synchronized void write(byte[] bytes, int off, int len) throws IOException {
        buf.put(bytes, off, len);
    }

}

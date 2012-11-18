package org.crunchr;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

/**
 * This is meant to be supported on both java and R end so that stuff serialized
 * to/from R raw buffers could be construed the same way on both ends.
 * 
 * @author dmitriy
 * 
 * @param <T>
 */
public abstract class RType<T> {
    /**
     * serialize given instance into the byte buffer.
     * 
     * @param buffer
     *            the receiving storage
     * @param src
     *            the object to be serialized
     */
    abstract public void set(ByteBuffer buffer, T src) throws IOException;

    /**
     * deserializes the instance of <code>T</code>
     * 
     * @param buffer
     * @param value
     *            holder. Optional. If supplied and supported (like in case of
     *            {@link Writable}) then it is filled with the value and
     *            returned as deserialized result.
     * 
     * @return the deserialized instance of <code>T</code>.
     * 
     */
    abstract public T get(ByteBuffer buffer, T holder) throws IOException;

    /**
     * 
     * If type is a "multi-emit" one, the convention is that T is actually an
     * array (or, in R speak, a dense vector) of elements (i.e.
     * <code>T extends Object[]</code>). Since in R, all primitive types are
     * actually arrays (i.e. there's no direct correspondence to a
     * java.lang.String type, only to a String[] type) then multi-emit types
     * actually make more sense then just a single Text or IntWritable. <R>
     * 
     * It also means that each array element will be output separately by the
     * pipe.
     * <P>
     * 
     * distinguish if R vector is meant to be emitted as a a whole writable, or
     * a series of elements in case of dense vectors. In the latter case, the
     * multi-emit type capability is needed.
     * <P>
     * 
     * @return true if we want to emit each value in a vector as a separate
     *         value.
     * 
     */
    public boolean isMultiEmit() {
        return false;
    }
}

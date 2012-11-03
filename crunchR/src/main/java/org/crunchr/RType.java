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
public interface RType<T> {
	/**
	 * serialize given instance into the byte buffer.
	 * 
	 * @param buffer
	 *            the receiving storage
	 * @param src
	 *            the object to be serialized
	 */
	void set(ByteBuffer buffer, T src) throws IOException;

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
	T get(ByteBuffer buffer, T holder) throws IOException;

}

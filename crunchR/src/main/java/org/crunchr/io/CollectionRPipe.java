package org.crunchr.io;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.crunch.Emitter;
import org.crunchr.RType;
import org.rosuda.JRI.Rengine;

/**
 * Data xchg pipe between Java and R side. We try to bulk up the transmission
 * using 2 buffers so that jni expenses become ultimately dilluted in the cost
 * of computations themselves at expense of some memory for the buffering.
 * <P>
 * 
 * The assumption is that the pipe is double-buffered and double-threaded (while
 * R thread consumes one buffer, another buffer is being prepp'd so potentially
 * we saturate CPU and memory bus better if there are currently available
 * multicore pockets of resource).
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class CollectionRPipe<S, T> {

	private ByteBuffer buffers[];
	private RType<S> srtype;
	private RType<T> trtype;
	private BlockingQueue<byte[]> outQueue;
	private Rengine rengine;
	private int outBuffer = 0;
	private int outCount = 0;
	private int availableOutBuffers;
	private Emitter<T> emitter;

	public CollectionRPipe(RType<S> srcRtype, RType<T> targetRtype,
			Emitter<T> emitter,
			int initialCapacity, Rengine rengine) {
		super();
		this.emitter=emitter;
		srtype = srcRtype;
		trtype = targetRtype;
		buffers = new ByteBuffer[] { ByteBuffer.allocate(initialCapacity + 2),
				ByteBuffer.allocate(initialCapacity + 2) };
		for (ByteBuffer bb : buffers)
			resetBuffer(bb);
		availableOutBuffers = buffers.length;

		outQueue = new ArrayBlockingQueue<byte[]>(buffers.length);
	}

	public void add(S value) throws IOException {
		if (outBuffer < 0) {

		}
		int position = buffers[outBuffer].position();
		while (true)
			try {
				srtype.set(buffers[outBuffer], value);
				outCount++;
				break;
			} catch (BufferOverflowException exc) {
				if (outCount == 0) {
					buffers[outBuffer] = ByteBuffer.allocate(buffers[outBuffer]
							.capacity() << 1);
					continue;
				} else {
					buffers[outBuffer].position(position);
					flushOutBuffer();
					position = buffers[outBuffer].position();
					continue;
				}
			}
		if (outCount == 0x7FFF)
			flushOutBuffer();
	}

	public void rcallbackOutBufferAvailable() throws IOException {
		synchronized (this) {
			availableOutBuffers++;
		}
	}
	
	public void rcallbackEmitBufferAvailable(byte[] emitBuff) throws IOException {
		
		
	}

	private static void resetBuffer(ByteBuffer buffer) {
		buffer.clear();
		buffer.position(2);
	}

	private void flushOutBuffer() throws IOException {

		ByteBuffer bb = buffers[outBuffer];
		bb.flip();
		bb.putShort((short) outCount);
		bb.position(0);
		synchronized (this) {
			availableOutBuffers--;
		}
		try {
			outQueue.put(bb.array());
			/*
			 * wait until at least one buffer is available again
			 */
			synchronized (this) {
				while (availableOutBuffers == 0)
					wait();
			}
		} catch (InterruptedException exc) {
			throw new IOException("Interrupted");
		}

	}

}

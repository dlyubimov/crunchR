package org.crunchr.io;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.crunch.Emitter;
import org.crunchr.RType;

/**
 * Data xchg pipes between Java and R side.
 * <P>
 * 
 * 
 * First pipe, from task to R side, is "input" pipeline. We try to bulk up the
 * transmission using 2 buffers so that jni expenses become ultimately dilluted
 * in the cost of computations themselves at expense of some memory for the
 * buffering.
 * <P>
 * 
 * The assumption is that the pipe is double-buffered and double-threaded (while
 * R thread consumes one buffer, another buffer is being prepp'd so potentially
 * we saturate CPU and memory bus better if there are currently available
 * multicore pockets of resource).
 * <P>
 * 
 * The second pipe is reverse, from R side to the emit function.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class DeprecatedTwoWayRPipe<S, T> {

	private static final int MAXINBUFCAPACITY = 1 << 12;

	private ByteBuffer inBuffers[];
	private RType<S> srtype;
	private RType<T> trtype;
	private BlockingQueue<byte[]> inQueue;
	private BlockingQueue<byte[]> outQueue;
	private boolean outClosed;
	// private Rengine rengine;
	private int inBuffer = 0;
	private int inCount = 0;
	private int availableOutBuffers;
	private Emitter<T> emitter;
	private final int flushBufferCapacity;

	public DeprecatedTwoWayRPipe(RType<S> srcRtype, RType<T> targetRtype,
			Emitter<T> emitter, int initialCapacity /* , Rengine rengine */) {
		super();
		this.emitter = emitter;
		srtype = srcRtype;
		trtype = targetRtype;
		inBuffers = new ByteBuffer[] {
				ByteBuffer.allocate(initialCapacity + 2),
				ByteBuffer.allocate(initialCapacity + 2) };
		for (ByteBuffer bb : inBuffers)
			resetBuffer(bb);
		availableOutBuffers = inBuffers.length;

		inQueue = new ArrayBlockingQueue<byte[]>(inBuffers.length);
		outQueue = new ArrayBlockingQueue<byte[]>(2);

		flushBufferCapacity = MAXINBUFCAPACITY;

	}

	public void add(S value) throws IOException {
		checkOutputQueue(false);
		int position = inBuffers[inBuffer].position();
		while (true)
			try {
				srtype.set(inBuffers[inBuffer], value);
				inCount++;
				break;
			} catch (BufferOverflowException exc) {
				if (inCount == 0) {
					inBuffers[inBuffer] = ByteBuffer
							.allocate(inBuffers[inBuffer].capacity() << 1);
					continue;
				} else {
					inBuffers[inBuffer].position(position);
					flushOutBuffer();
					position = inBuffers[inBuffer].position();
					continue;
				}
			}
		if (inCount == 0x7FFF
				|| inBuffers[inBuffer].capacity() >= flushBufferCapacity)
			flushOutBuffer();
	}

	/**
	 * this actually just pushes end-of-queue to the R side in the input
	 * pipeline without waiting for R side take any notice or stop processing.
	 * 
	 * @throws IOException
	 */
	public void closeInput() throws IOException {
		flushOutBuffer();
		sendClose();
	}

	/**
	 * close emitting pipeline from R to to the emitter.
	 * 
	 * @param waitClose
	 *            if true, wait till pipeline is closed on the R side and then
	 *            gracefully terminate.
	 * @throws IOException
	 */
	public void closeOutput(boolean waitClose) throws IOException {
		if (waitClose)
			checkOutputQueue(true);
		else
			outClosed = true;
	}

	/**
	 * this is called by R thread to signal that it finished consuming buffer
	 * from a queue so we can proceed filling it up again on the java side of
	 * things.
	 * 
	 * @throws IOException
	 */
	public synchronized void rcallbackBufferConsumed() throws IOException {
		availableOutBuffers++;
	}

	/**
	 * R thread calls that to emit byte array packed buffer.
	 * 
	 * @param emitBuff
	 * @throws IOException
	 */
	public void rcallbackEmitBuffer(byte[] emitBuff) throws IOException {
		/*
		 * so we are in the context of an R thread. I assuming JNI is passing
		 * buffer by value here. so R side can safely reuse its output buffer
		 * without side effects on our side.
		 */
		try {
			outQueue.put(emitBuff);
		} catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}
	}

	/**
	 * R thread calls this to retrieve next available chunk of task input (pull
	 * interface)
	 * 
	 * @return
	 * @throws IOException
	 */
	public byte[] rcallbackNextBuff() throws IOException {
		try {
			/*
			 * TODO: use timeouts! this is prototype-quality.
			 */
			byte[] input = inQueue.take();
			if (input.length == 0)
				/* end of input */
				return null;
			return input;

		} catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

	}

	private static void resetBuffer(ByteBuffer buffer) {
		buffer.clear();
		buffer.position(2);
	}

	private void flushOutBuffer() throws IOException {

		ByteBuffer bb = inBuffers[inBuffer];
		if (bb.position() == 2)
			// nothing written, no need to do anything
			return;

		bb.flip();
		bb.putShort((short) inCount);
		bb.position(0);
		synchronized (this) {
			availableOutBuffers--;
		}
		try {
			inQueue.put(bb.array());
			/*
			 * wait until at least one buffer is available again
			 */
			synchronized (this) {
				while (availableOutBuffers == 0)
					wait();
			}
			inBuffer = (inBuffer + 1) % inBuffers.length;
			resetBuffer(inBuffers[inBuffer]);
		} catch (InterruptedException exc) {
			throw new IOException("Interrupted");
		}
	}

	private void sendClose() throws IOException {
		try {
			inQueue.put(new byte[0]);
		} catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}
	}

	/**
	 * checks output queue, non-blocking fashion.
	 * 
	 * @return true if ok, false if output end was closd.
	 * @throws IOException
	 */
	private boolean checkOutputQueue(boolean waitClose) throws IOException {
		if (outClosed)
			return false;
		byte[] emitBuff;
		try {
			while (null != (emitBuff = waitClose ? outQueue.take() : outQueue
					.poll())) {
				ByteBuffer bb = ByteBuffer.wrap(emitBuff);
				int n = bb.getShort();
				if (n == 0) {
					outClosed = true;
					return false;
				}

				T holder = null;
				for (int i = 0; i < n; i++) {
					holder = trtype.get(bb, holder);
					emitter.emit(holder);
				}
			}
		} catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}
		return true;
	}

}

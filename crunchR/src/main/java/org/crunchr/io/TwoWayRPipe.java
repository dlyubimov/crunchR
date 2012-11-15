package org.crunchr.io;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import org.crunchr.RType;
import org.crunchr.fn.RDoFn;
import org.crunchr.fn.RDoFnRType;
import org.crunchr.r.RCallException;
import org.crunchr.r.RController;

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
public class TwoWayRPipe {

    private static final int      MAXINBUFCAPACITY = 1 << 12;

    /*
     * special message: add a do Fn to the R side
     */
    private static final int      ADD_DO_FN        = -1;

    private static final Logger   s_log            = Logger.getLogger("crunchR");

    private ByteBuffer            inBuffers[];
    private BlockingQueue<byte[]> inQueue;
    private BlockingQueue<byte[]> outQueue;
    private boolean               outClosed;
    // private Rengine rengine;
    private int                   inBuffer         = 0;
    private int                   inCount          = 0;
    private int                   availableOutBuffers;
    private final int             flushBufferCapacity;
    private Thread                rThread;
    private RCallException        lastErr;

    @SuppressWarnings("rawtypes")
    private RType<RDoFn>          rDoFnType        = new RDoFnRType();

    @SuppressWarnings("rawtypes")
    private Map<Integer, RDoFn>   doFnMap          = new HashMap<Integer, RDoFn>();
    private Map<Integer, Object>  outHolders       = new HashMap<Integer, Object>();

    public TwoWayRPipe(int initialCapacity, RController controller) throws IOException, RCallException {
        super();
        inBuffers =
            new ByteBuffer[] { ByteBuffer.allocate(initialCapacity + 2), ByteBuffer.allocate(initialCapacity + 2) };
        /*
         * it is actually more convenient to work with LE byte order on the R side, so 
         * we enforce LE here. 
         */
        for (ByteBuffer bb : inBuffers) { 
            bb.order(ByteOrder.LITTLE_ENDIAN);
            resetBuffer(bb);
        }
        availableOutBuffers = inBuffers.length;

        inQueue = new ArrayBlockingQueue<byte[]>(inBuffers.length);
        outQueue = new ArrayBlockingQueue<byte[]>(2);

        flushBufferCapacity = MAXINBUFCAPACITY;
        controller.evalWithMethodArgs("crunchR.rpipe <- crunchR.TwoWayPipe$new", this);

    }

    public void addDoFn(RDoFn<?, ?> doFn) throws IOException {
        int doFnRef = doFnMap.size() + 1;
        doFnMap.put(doFnRef, doFn);
        doFn.setDoFnRef(doFnRef);

        // ... dispatch function addition to the R side
        add(doFn, rDoFnType, ADD_DO_FN);
    }

    public <S> void add(S value, RType<S> rtype, int doFnRef) throws IOException {
        checkOutputQueue(false);
        int position = inBuffers[inBuffer].position();
        ByteBuffer bb = inBuffers[inBuffer];
        while (true)
            try {
                SerializationHelper.setVarUint32(bb, doFnRef);
                rtype.set(inBuffers[inBuffer], value);
                inCount++;
                break;
            } catch (BufferOverflowException exc) {
                if (inCount == 0) {
                    inBuffers[inBuffer] = bb = ByteBuffer.allocate(inBuffers[inBuffer].capacity() << 1);
                    continue;
                } else {
                    bb.position(position);
                    flushOutBuffer();
                    bb = inBuffers[inBuffer];
                    position = bb.position();
                    continue;
                }
            }
        if (inCount == 0x7FFF || inBuffers[inBuffer].position() >= flushBufferCapacity)
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

    /**
     * BIG WARNING: this starts pipe on the R side. It means that R will keep
     * scanning for incoming messages or shutdown requests and new R calls will
     * not be available until the pipe is shut down.
     */
    public void start() {
        Runnable r = new Runnable() {

            @Override
            public void run() {
                // should be started by now
                try {
                    RController controller = RController.getInstance(null);
                    controller.eval("crunchR.rpipe$run()");
                } catch (IOException exc) {
                    lastErr = new RCallException(exc);
                    s_log.severe(exc.toString());

                } catch (RCallException exc) {
                    lastErr = exc;
                    s_log.severe(exc.toString());
                }

            }

        };
        rThread = new Thread(r);
        rThread.start();
    }

    public void shutdown(boolean waitClose) throws IOException {
        closeInput();
        closeOutput(false);
        try {
            if (waitClose)
                rThread.join();
            if (lastErr != null)
                throw new IOException(lastErr);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted");
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
            while (null != (emitBuff = waitClose ? outQueue.take() : outQueue.poll())) {
                ByteBuffer bb = ByteBuffer.wrap(emitBuff);
                int n = bb.getShort();
                if (n == 0) {
                    outClosed = true;
                    return false;
                }

                Object holder = null;
                for (int i = 0; i < n; i++) {
                    int doFnRef = SerializationHelper.getVarUint32(bb);

                    @SuppressWarnings("unchecked")
                    RDoFn<Object, Object> rDoFn = doFnMap.get(doFnRef);
                    if (rDoFn == null) {
                        throw new IOException("Unknown doFnRef:" + doFnRef);
                    }

                    RType<Object> trtype = rDoFn.getTRType();
                    holder = outHolders.get(doFnRef);
                    boolean doSave = holder == null;
                    holder = trtype.get(bb, holder);
                    if (doSave) {
                        outHolders.put(doFnRef, holder);
                    }
                    rDoFn.getEmitter().emit(holder);

                }
            }
        } catch (InterruptedException e) {
            throw new IOException("Interrupted", e);
        }
        return true;
    }

}

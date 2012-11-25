package org.crunchr.types.io;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.crunch.Pair;
import org.crunchr.fn.CleanupDoFn;
import org.crunchr.fn.RDoFn;
import org.crunchr.fn.RDoFnRType;
import org.crunchr.r.RCallException;
import org.crunchr.r.RController;
import org.crunchr.types.RType;

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

    private static final Logger   s_log              = Logger.getLogger("crunchR");

    private static final int      MAXINBUFCAPACITY   = 1 << 12;
    /*
     * special message: add a do Fn to the R side
     */
    public static final int       ADD_DO_FN          = -1;
    public static final int       CLEANUP_FN         = -2;

    private ByteBuffer            inBuffers[];
    private BlockingQueue<byte[]> inQueue;
    private BlockingQueue<byte[]> outQueue;
    private boolean               outClosed;
    // private Rengine rengine;
    private int                   inBuffer           = 0;
    private int                   inCount            = 0;
    private int                   availableOutBuffers;
    private final int             flushBufferCapacity;
    private Thread                rThread;
    private RCallException        lastErr;

    @SuppressWarnings("rawtypes")
    private RType<RDoFn>          rDoFnType          = new RDoFnRType();

    @SuppressWarnings("rawtypes")
    private Map<Integer, RDoFn>   doFnMap            = new HashMap<Integer, RDoFn>();
    private Map<Integer, Object>  outHolders         = new HashMap<Integer, Object>();

    /*
     * 5 minutes max wait to close input queue by default
     */
    private int                   inputQueueMaxWait  = 6 * 5;
    /*
     * 5 minutes max wait to close output queue by default
     */
    private int                   outputQueueMaxWait = 60 * 5;

    public TwoWayRPipe(int initialCapacity, RController controller) throws IOException, RCallException {
        super();
        inBuffers =
            new ByteBuffer[] { ByteBuffer.allocate(initialCapacity + 2), ByteBuffer.allocate(initialCapacity + 2) };
        /*
         * it is actually more convenient to work with LE byte order on the R
         * side, so we enforce LE here.
         */
        for (ByteBuffer bb : inBuffers) {
            bb.order(ByteOrder.LITTLE_ENDIAN);
            resetBuffer(bb);
        }
        availableOutBuffers = inBuffers.length;

        inQueue = new ArrayBlockingQueue<byte[]>(inBuffers.length);
        outQueue = new ArrayBlockingQueue<byte[]>(2);

        flushBufferCapacity = MAXINBUFCAPACITY;

        /*
         * special function: processing cleanup receipts
         */
        CleanupDoFn cleanupDoFn = new CleanupDoFn();
        cleanupDoFn.setRpipe(this);
        doFnMap.put(CLEANUP_FN, cleanupDoFn);

        controller.evalWithMethodArgs("crunchR.rpipe <- crunchR.TwoWayPipe$new", this);

    }

    public void addDoFn(RDoFn<?, ?> doFn) throws IOException {
        int doFnRef = doFnMap.size() + 1;
        doFnMap.put(doFnRef, doFn);
        doFn.setDoFnRef(doFnRef);
        doFn.setRpipe(this);

        // ... dispatch function addition to the R side
        add(doFn, rDoFnType, ADD_DO_FN);
    }

    public <S> void add(S value, RType<S> rtype, int doFnRef) throws IOException {
        checkOutputQueue(false, false);
        int position = inBuffers[inBuffer].position();
        ByteBuffer bb = inBuffers[inBuffer];
        while (true)
            try {
                SerializationHelper.setVarInt32(bb, doFnRef);
                rtype.set(inBuffers[inBuffer], value);
                inCount++;
                break;
            } catch (BufferOverflowException exc) {
                if (inCount == 0) {
                    inBuffers[inBuffer] = bb = ByteBuffer.allocate(inBuffers[inBuffer].capacity() << 1);
                    bb.order(ByteOrder.LITTLE_ENDIAN);
                    resetBuffer(bb);
                    continue;
                } else {
                    bb.position(position);
                    flushInput();
                    bb = inBuffers[inBuffer];
                    position = bb.position();
                    continue;
                }
            }
        if (inCount == 0x7FFF || inBuffers[inBuffer].position() >= flushBufferCapacity)
            flushInput();
    }

    /**
     * this actually just pushes end-of-queue to the R side in the input
     * pipeline without waiting for R side take any notice or stop processing.
     * 
     * @throws IOException
     */
    public void closeInput() throws IOException {
        flushInput();
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
            checkOutputQueue(true, false);
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
        notify();
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
            synchronized(this) { 
                notify();
            }
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
     * Not thread-safe call. but should be no problem for single-threaded MR
     * tasks.
     */
    public void startIfNotStarted() {
        if (rThread == null)
            start();
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
        closeOutput(waitClose);
        try {
            if (waitClose)
                rThread.join();
            if (lastErr != null)
                throw new IOException(lastErr);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted");
        }
    }

    /**
     * checks output queue, non-blocking fashion.
     * <P>
     * 
     * Warning: this must be re-entrant since calls to emit() may cause nested
     * invocations of this method!
     * <P>
     * 
     * @param once
     *            TODO
     * 
     * @return true if ok, false if output end was closd.
     * @throws IOException
     */
    public boolean checkOutputQueue(boolean wait, boolean once) throws IOException {
        if (outClosed)
            return false;
        byte[] emitBuff;
        try {
            for (int z = 0; !once || z < 1; z++) {

                int secondsWaited = 0;
                int waitTime = 20;
                boolean threadExited = false;

                /*
                 * obtain next buffer
                 */
                while (null == (emitBuff = wait ? outQueue.poll(waitTime, TimeUnit.SECONDS) : outQueue.poll())) {
                    if (!wait)
                        return true;
                    secondsWaited += waitTime;
                    if (secondsWaited >= outputQueueMaxWait)
                        throw new IOException("R side is not responding in the maximum amount of time allowed.");
                    if (!rThread.isAlive()) {
                        /*
                         * if we see thread having exited it is still possible
                         * that a normal output closure message is pending. so
                         * we make sure we check again for the graceful message
                         * before stating it was unexpected R-side breakage.
                         */
                        if (threadExited) {
                            throw lastErr != null ? new IOException("R side exited prematurely", lastErr)
                                : new IOException("R side exited prematurely (no errors were captured)");
                        } else {
                            threadExited = true;
                            /* and try to wait again for the normal shutdown. */
                        }
                    }
                }

                /*
                 * process next emit buff
                 */
                ByteBuffer bb = ByteBuffer.wrap(emitBuff).order(ByteOrder.LITTLE_ENDIAN);
                if (bb.remaining() == 0) {
                    outClosed = true;
                    return false;
                }
                int n = bb.getShort();

                for (int i = 0; i < n; i++) {
                    Object holder = null;
                    int doFnRef = SerializationHelper.getVarInt32(bb);

                    @SuppressWarnings("unchecked")
                    RDoFn<Object, Object> rDoFn = doFnMap.get(doFnRef);
                    if (rDoFn == null) {
                        throw new IOException("Unknown doFnRef:" + doFnRef);
                    }

                    RType<Object> trtype = rDoFn.getTRType();
                    boolean multiEmit = trtype.isMultiEmit();
                    boolean doSave = false;

                    if (!multiEmit) {
                        holder = outHolders.remove(doFnRef);
                        doSave = true;
                    }
                    holder = trtype.get(bb, holder);
                    if (holder != null) {
                        if (trtype.isMultiEmit()) {
                            /*
                             * we don't want to keep array as a holder around.
                             * so we don't put it back to the outHolder reusable
                             * collection. Not sure if it was worth the trouble
                             * in the first place, but some small items such as
                             * ints, it would...
                             */
                            if (holder instanceof Pair) {

                                /*
                                 * PTable multi-emit types emit
                                 * Pair<Object[],Object[]>, not
                                 * Pair<Object,Object>[] to save on number of
                                 * java references generated. So we need this
                                 * code to adapt that to behavior Crunch
                                 * actually expects. It is still pretty wasteful
                                 * in case such as Double[] on java side which
                                 * could be much better off represented by
                                 * double[] to reduce java reference overhead...
                                 * we will have to address these special cases
                                 * later. However, it is only a case for
                                 * dense+unnamed R vectors so it is not really
                                 * that conducive to R vector serialization as
                                 * it might seem at first sight. So I put it off
                                 * until i have a better idea how to adapt
                                 * those.
                                 */
                                @SuppressWarnings({ "unchecked", "rawtypes" })
                                Pair<Object[], Object[]> p = (Pair) holder;
                                Object[] key = p.first();
                                Object[] value = p.second();
                                if (key.length != value.length)
                                    throw new IOException("multi-emit key/value elements must be of the same length");
                                int len = key.length;
                                for (int j = 0; j < len; j++)
                                    rDoFn.getEmitter().emit(Pair.of(key[j], value[j]));
                            } else {
                                /* multi-emit non-ptable rType */
                                for (Object item : (Object[]) holder) {
                                    rDoFn.getEmitter().emit(item);
                                }
                            }
                        } else {
                            /* single-emit */
                            if (doSave) {
                                outHolders.put(doFnRef, holder);
                            }
                            rDoFn.getEmitter().emit(holder);
                        }
                    }
                }
            }
            return true;
        } catch (InterruptedException e) {
            throw new IOException("Interrupted", e);
        }
    }

    public RDoFn<?, ?> findDoFn(int doFnRef) {
        return doFnMap.get(doFnRef);
    }

    public void flushInput() throws IOException {

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
            if (!inQueue.offer(bb.array(), inputQueueMaxWait, TimeUnit.SECONDS))
                throw new IOException("Unable to flush input queue and maximum timeout expred.");
            /*
             * wait until at least one buffer is available again
             */
            synchronized (this) {
                while (availableOutBuffers == 0) {
                    checkOutputQueue(false, false);
                    wait();
                }
            }
            inBuffer = (inBuffer + 1) % inBuffers.length;
            resetBuffer(inBuffers[inBuffer]);
            inCount = 0;
        } catch (InterruptedException exc) {
            throw new IOException("Interrupted");
        }
    }

    private static void resetBuffer(ByteBuffer buffer) {
        buffer.clear();
        buffer.position(2);
    }

    private void sendClose() throws IOException {
        try {
            inQueue.put(new byte[0]);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted", e);
        }
    }

}

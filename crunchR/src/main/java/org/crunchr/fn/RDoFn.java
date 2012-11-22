package org.crunchr.fn;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.crunchr.r.RController;
import org.crunchr.types.RType;
import org.crunchr.types.io.RVarInt32;
import org.crunchr.types.io.TwoWayRPipe;

/**
 * R's doFn
 * 
 * @author dmitriy
 * 
 * @param <S>
 *            source type
 * @param <T>
 *            output type
 */
public class RDoFn<S, T> extends DoFn<S, T> {

    private static final long       serialVersionUID = 1L;

    protected byte[]                closureList;
    protected String                srtypeClassName, trtypeClassName;
    protected String                srtypeJavaClassName, trtypeJavaClassName;

    protected transient TwoWayRPipe rpipe;
    protected transient int         doFnRef;
    protected transient RType<S>    srtype;
    protected transient RType<T>    trtype;
    protected transient Emitter<T>  emitter;
    protected transient boolean     cleaned;

    /**
     * deserializing factory method
     * 
     * @return doFn
     */
    @SuppressWarnings("rawtypes")
    public static RDoFn fromBytes(byte[] bytes) throws IOException {
        return RDoFnRType.getInstance().get(ByteBuffer.wrap(bytes), null);
    }

    public byte[] getClosureList() {
        return closureList;
    }

    public void setClosureList(byte[] closureList) {
        this.closureList = closureList;
    }


    public TwoWayRPipe getRpipe() {
        return rpipe;
    }

    public void setRpipe(TwoWayRPipe rpipe) {
        this.rpipe = rpipe;
    }

    public int getDoFnRef() {
        return doFnRef;
    }

    public void setDoFnRef(int doFnRef) {
        this.doFnRef = doFnRef;
    }

    public String[] getRTypeClassNames() {
        return new String[] { srtypeClassName, trtypeClassName };
    }

    public void setRTypeClassNames(String[] classNames) {
        srtypeClassName = classNames[0];
        trtypeClassName = classNames[1];
    }

    public void setRTypeJavaClassNames(String[] classNames) {
        srtypeJavaClassName = classNames[0];
        trtypeJavaClassName = classNames[1];
    }

    public RType<S> getSRType() {
        return srtype;
    }

    public RType<T> getTRType() {
        return trtype;
    }

    public Emitter<T> getEmitter() {
        return emitter;
    }

    public boolean isCleaned() {
        return cleaned;
    }

    public void setCleaned(boolean cleaned) {
        this.cleaned = cleaned;
    }

    @Override
    public void process(S src, Emitter<T> emitter) {
        try {
            this.emitter = emitter;
            rpipe.add(src, srtype, doFnRef);
        } catch (IOException exc) {
            // TODO: so what are we supposed to wrap it to in Crunch?
            throw new RuntimeException(exc);
        }

    }

    @Override
    public void cleanup(Emitter<T> emitter) {
        try {
            /*
             * ad cleanup message for our function to the input queue
             */
            rpipe.add(doFnRef, RVarInt32.getInstance(), TwoWayRPipe.CLEANUP_FN);
            /*
             * flush/dispatch input buffer (since we are trying to wait till R
             * side definitely has processed the cleanup. Indeed, cleanup on R
             * side may cause some emitter flushing too, so we have to make sure
             * everything is emitted and R side cleanup is finalized before we we
             * confirm this function is completely done, back to Crunch.
             */
            rpipe.flushInput();
            /*
             * wait till our R counterpart finishes all the things it still
             * wanted to finish...
             */
            while (!cleaned)
                rpipe.checkOutputQueue(true);
            /*
             * after we got a receipt of cleanup activity for this function from
             * the R side, we can confirm the same to Crunch now and exit.
             */
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void initialize() {
        super.initialize();
        try {
            ClassLoader cloader = Thread.currentThread().getContextClassLoader();
            Class<? extends RType> srtypeClass = cloader.loadClass(srtypeJavaClassName).asSubclass(RType.class);
            Class<? extends RType> trtypeClass = cloader.loadClass(trtypeJavaClassName).asSubclass(RType.class);
            srtype = ReflectionUtils.newInstance(srtypeClass, getConfiguration());
            trtype = ReflectionUtils.newInstance(trtypeClass, getConfiguration());

            /*
             * for the purpose of evaluation of approach, we are going to make
             * another assumption and assume that no initialize() is going to be
             * called before any process() of any function. This, however, a
             * more dangerous assumption in general case since Crunch may develop
             * in ways that obviously do not hold this assumption true, or it
             * may lazily delay function initializations in some cases for the
             * purposes of optimization or something. Going forward, we probably
             */
            RController rcontroller = RController.getInstance(getConfiguration());
            rpipe = rcontroller.getRPipe();
            rpipe.startIfNotStarted();

            rpipe.addDoFn(this);

        } catch (ClassNotFoundException exc) {
            throw new RuntimeException(exc);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

}

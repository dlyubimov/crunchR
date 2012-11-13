package org.crunchr.fn;

import java.io.IOException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.crunchr.RType;
import org.crunchr.io.TwoWayRPipe;

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

    protected byte[]                rInitializeFun, rCleanupFun, rProcessFun;
    protected String                srtypeClassName, trtypeClassName;

    protected transient TwoWayRPipe rpipe;
    protected transient int         doFnRef;
    protected transient RType<S>    srtype;
    protected transient RType<T>    trtype;
    protected transient Emitter<T>  emitter;

    public byte[] getrInitializeFun() {
        return rInitializeFun;
    }

    public void setrInitializeFun(byte[] rInitializeFun) {
        this.rInitializeFun = rInitializeFun;
    }

    public byte[] getrCleanupFun() {
        return rCleanupFun;
    }

    public void setrCleanupFun(byte[] rCleanupFun) {
        this.rCleanupFun = rCleanupFun;
    }

    public byte[] getrProcessFun() {
        return rProcessFun;
    }

    public void setrProcessFun(byte[] rProcessFun) {
        this.rProcessFun = rProcessFun;
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

    public RType<S> getSRType() {
        return srtype;
    }

    public RType<T> getTRType() {
        return trtype;
    }

    public Emitter<T> getEmitter() {
        return emitter;
    }

    @Override
    public void process(S src, Emitter<T> emitter) {
        try {
            if (rpipe == null)
                lazySetup(emitter);
            rpipe.add(src, srtype, doFnRef);
        } catch (IOException exc) {
            // TODO: so what are we supposed to wrap it to in Crunch?
            throw new RuntimeException(exc);
        }

    }

    @Override
    public void cleanup(Emitter<T> emitter) {
        super.cleanup(emitter);
    }

    /**
     * we assume emitter doesn't change during all calls to
     * {@link #process(Object, Emitter)}.
     * 
     * @param emitter
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void lazySetup(Emitter<T> emitter) throws IOException {
        try {
            ClassLoader cloader = Thread.currentThread().getContextClassLoader();
            Class<? extends RType> srtypeClass = cloader.loadClass(srtypeClassName).asSubclass(RType.class);
            Class<? extends RType> trtypeClass = cloader.loadClass(trtypeClassName).asSubclass(RType.class);
            srtype = ReflectionUtils.newInstance(srtypeClass, getConfiguration());
            trtype = ReflectionUtils.newInstance(trtypeClass, getConfiguration());
        } catch (ClassNotFoundException exc) {
            throw new IOException(exc);
        }
        this.emitter = emitter;

    }

}

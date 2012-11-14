package org.crunchr.fn;

import java.io.IOException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.crunchr.RType;
import org.crunchr.io.TwoWayRPipe;
import org.crunchr.r.RController;

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
    protected String                srtypeJavaClassName, trtypeJavaClassName;

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
    
    public void setRTypeClassNames(String[] classNames) {
        srtypeClassName=classNames[0];
        trtypeClassName=classNames[1];
    }
    
    public void setRTypeJavaClassNames(String[] classNames) { 
        srtypeJavaClassName=classNames[0];
        trtypeJavaClassName=classNames[1];
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
            this.emitter = emitter;
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
             * more dangerous asumption in general case since Crunch may develop
             * in ways that obviously do not hold this assumption true, or it
             * may lazily delay function initalizations in some cases for the
             * purposes of optimization or something. Going forward, we probably
             */
            RController rcontroller = RController.getInstance(getConfiguration());
            rpipe = rcontroller.getRPipe();

            rpipe.addDoFn(this);

        } catch (ClassNotFoundException exc) {
            throw new RuntimeException(exc);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

}

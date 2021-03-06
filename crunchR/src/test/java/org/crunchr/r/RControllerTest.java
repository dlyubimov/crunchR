package org.crunchr.r;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;

import org.crunchr.fn.RDoFn;
import org.crunchr.fn.RDoFnRType;
import org.crunchr.types.RTypeState;
import org.crunchr.types.io.RString;
import org.crunchr.types.io.TwoWayRPipe;
import org.rosuda.JRI.REXP;
import org.testng.annotations.Test;

public class RControllerTest {

    @Test(enabled = false)
    public void test1() throws Exception {
        RController rController = RController.getInstance(null);
        TwoWayRPipe rpipe = rController.getRPipe();
        rpipe.start();
        rpipe.shutdown(true);

    }

    @Test(enabled = true)
    public void test2() throws Exception {
        RController rController = RController.getInstance(null);

        // simulate java-side RDoFn at backend

        final byte[][] fser = new byte[1][];
        Object callback = new Object() {
            public void set(byte[] ser) {
                fser[0] = ser;
            }
        };

        REXP callbackRef = rController.getEngine().createRJavaRef(callback);
        rController.getEngine().assign("callback", callbackRef);

        rController.eval("callback$set( .jarray(serialize(list(process=function(x) x), connection=NULL)))");

        RDoFn<String, String> simFun = new RDoFn<String, String>();
        simFun.setClosureList(fser[0]);
        simFun.setDoFnRef(255);
        RTypeState rtypeState = new RTypeState();
        rtypeState.setRClassName("RString");
        rtypeState.setJavaClassName(RString.class.getName());
        simFun.setsRTypeState(rtypeState);
        simFun.settRTypeState(rtypeState);

        // save the simfun R message for tests
        RDoFnRType fnrtype = new RDoFnRType();
        ByteBuffer buff = ByteBuffer.allocate(1 << 10);
        fnrtype.set(buff, simFun);
        buff.flip();
        FileOutputStream fos = new FileOutputStream("simfun.dat");
        fos.getChannel().write(buff);
        fos.close();

        TwoWayRPipe rpipe = rController.getRPipe();
        rpipe.startIfNotStarted();

        for ( int i = 0; i < 29; i++)
            rpipe.addDoFn(simFun);

        // simulate function addition and call on R side
        // ...

        rpipe.shutdown(true);

    }

}

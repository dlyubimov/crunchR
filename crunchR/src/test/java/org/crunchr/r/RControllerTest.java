package org.crunchr.r;

import org.crunchr.fn.RDoFn;
import org.crunchr.io.RString;
import org.crunchr.io.TwoWayRPipe;
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

    @Test
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

        rController.eval("callback$set( .jarray(serialize(function(x) x, connection=NULL)))");

        RDoFn<String, String> simFun = new RDoFn<String, String>();
        simFun.setrProcessFun(fser[0]);
        simFun.setRTypeClassNames(new String[] { "RString", "RString" });
        simFun.setRTypeJavaClassNames(new String[] { RString.class.getName(), RString.class.getName() });

        TwoWayRPipe rpipe = rController.getRPipe();
        rpipe.start();

        rpipe.addDoFn(simFun);
        

        // simulate function addition and call on R side
        // ...

        rpipe.shutdown(true);

    }

}

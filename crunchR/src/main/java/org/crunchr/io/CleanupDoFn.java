package org.crunchr.io;

import org.apache.crunch.Emitter;
import org.crunchr.fn.RDoFn;

public class CleanupDoFn extends RDoFn<Integer, Integer> {

    private static final long serialVersionUID = 1L;

    {
        doFnRef = TwoWayRPipe.CLEANUP_FN;
        trtype = RVarInt32.getInstance();
    }

    Emitter<Integer>          emitter          = new Emitter<Integer>() {

                                                   @Override
                                                   public void emit(Integer doFnRef) {
                                                       RDoFn<?, ?> doFn = rpipe.findDoFn(doFnRef);
                                                       if (doFn == null)
                                                           throw new RuntimeException(
                                                               String.format("Unknown cleanup fn ref %d.", doFnRef));
                                                       doFn.setCleaned(true);
                                                   }

                                                   @Override
                                                   public void flush() {

                                                   }

                                               };

    @Override
    public Emitter<Integer> getEmitter() {
        return emitter;
    }

}

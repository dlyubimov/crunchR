package org.crunchr.fn;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.types.RType;
import org.crunchr.types.RTypeState;
import org.crunchr.types.io.RRaw;
import org.crunchr.types.io.RStrings;
import org.crunchr.types.io.RTypeStateRType;
import org.crunchr.types.io.SerializationHelper;

/**
 * serialization of R function presentation between java and R
 * 
 * @author dmitriy
 * 
 */
@SuppressWarnings("rawtypes")
public class RDoFnRType extends RType<RDoFn> {

    /*
     * This is by far a stateless flyweight.. so we can use singleton pattern to
     * serve all
     */
    private static RDoFnRType singleton = new RDoFnRType();

    public static RDoFnRType getInstance() {
        return singleton;
    }

    @Override
    public void set(ByteBuffer buffer, RDoFn src) throws IOException {
        RRaw rawType = RRaw.getInstance();
        RTypeStateRType rtypeType = RTypeStateRType.getInstance();

        SerializationHelper.setVarUint32(buffer, src.getDoFnRef());
        rawType.set(buffer, src.getClosureList());
        rtypeType.set(buffer, src.getsRTypeState());
        rtypeType.set(buffer, src.gettRTypeState());

    }

    @Override
    public RDoFn get(ByteBuffer buffer, RDoFn holder) throws IOException {
        RRaw rawType = RRaw.getInstance();
        RTypeStateRType rtypeType = RTypeStateRType.getInstance();

        if (holder == null)
            holder = new RDoFn();

        RDoFn doFn = holder;
        doFn.setDoFnRef(SerializationHelper.getVarUint32(buffer));

        doFn.setClosureList(rawType.get(buffer, null));

        doFn.setsRTypeState(rtypeType.get(buffer, null));
        doFn.settRTypeState(rtypeType.get(buffer, null));

        return doFn;
    }

}

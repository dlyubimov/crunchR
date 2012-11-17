package org.crunchr.fn;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.RType;
import org.crunchr.io.RRaw;
import org.crunchr.io.RStrings;
import org.crunchr.io.SerializationHelper;

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

        SerializationHelper.setVarUint32(buffer, src.getDoFnRef());
        rawType.set(buffer, src.getrInitializeFun());
        rawType.set(buffer, src.getrProcessFun());
        rawType.set(buffer, src.getrCleanupFun());
        RStrings.getInstance().set(buffer, src.getRTypeClassNames());

    }

    @Override
    public RDoFn get(ByteBuffer buffer, RDoFn holder) throws IOException {
        RRaw rawType = RRaw.getInstance();
        RStrings stringsType = RStrings.getInstance();
        if (holder == null)
            holder = new RDoFn();

        RDoFn doFn = holder;
        doFn.setDoFnRef(SerializationHelper.getVarUint32(buffer));

        doFn.setrInitializeFun(rawType.get(buffer, null));
        doFn.setrProcessFun(rawType.get(buffer, null));
        doFn.setrCleanupFun(rawType.get(buffer, null));
        doFn.setRTypeClassNames(stringsType.get(buffer, null));
        doFn.setRTypeJavaClassNames(stringsType.get(buffer, null));

        return doFn;
    }

}

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
public class RDoFnRType implements RType<RDoFn> {

    @Override
    public void set(ByteBuffer buffer, RDoFn src) throws IOException {
        SerializationHelper.setVarUint32(buffer, src.getDoFnRef());
        RRaw.getInstance().set(buffer, src.getrInitializeFun());
        RRaw.getInstance().set(buffer, src.getrProcessFun());
        RRaw.getInstance().set(buffer, src.getrCleanupFun());
        RStrings.getInstance().set(buffer, src.getRTypeClassNames());

    }

    @Override
    public RDoFn get(ByteBuffer buffer, RDoFn holder) throws IOException {
        throw new UnsupportedOperationException();
    }

}

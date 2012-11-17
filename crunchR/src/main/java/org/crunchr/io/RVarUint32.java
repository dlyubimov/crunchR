package org.crunchr.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.RType;

public class RVarUint32 extends RType<Integer> {

    private static final RVarUint32 singleton = new RVarUint32();

    public static RVarUint32 getInstance() {
        return singleton;
    }

    @Override
    public void set(ByteBuffer buffer, Integer src) throws IOException {
        SerializationHelper.setVarUint32(buffer, src);
    }

    @Override
    public Integer get(ByteBuffer buffer, Integer holder) throws IOException {
        return SerializationHelper.getVarUint32(buffer);
    }

}

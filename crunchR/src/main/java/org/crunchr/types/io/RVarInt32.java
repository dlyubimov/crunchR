package org.crunchr.types.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.types.RType;

public class RVarInt32 extends RType<Integer> {

    private static final RVarInt32 singleton = new RVarInt32();

    public static RVarInt32 getInstance() {
        return singleton;
    }

    @Override
    public void set(ByteBuffer buffer, Integer src) throws IOException {
        SerializationHelper.setVarInt32(buffer, src);
    }

    @Override
    public Integer get(ByteBuffer buffer, Integer holder) throws IOException {
        return SerializationHelper.getVarInt32(buffer);
    }

}

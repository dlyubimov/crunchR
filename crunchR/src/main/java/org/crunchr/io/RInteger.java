package org.crunchr.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.RType;

public class RInteger extends RType<Integer> {

    private static final RInteger singleton = new RInteger();

    public static RInteger getInstance() {
        return singleton;
    }

    @Override
    public void set(ByteBuffer buffer, Integer src) throws IOException {
        buffer.putInt(src);
    }

    @Override
    public Integer get(ByteBuffer buffer, Integer holder) throws IOException {
        return buffer.getInt();
    }


}

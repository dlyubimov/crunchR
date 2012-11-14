package org.crunchr.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.RType;

/**
 * short raw vectors. vector of length 0 will be considered the same as null to
 * save on pointless new byte[0] java reference allocations.
 * 
 * @author dmitriy
 * 
 */
public class RRaw implements RType<byte[]> {

    private static final RRaw singleton = new RRaw();

    public static RRaw getInstance() {
        return singleton;
    }

    @Override
    public void set(ByteBuffer buffer, byte[] src) throws IOException {
        int len = src == null ? 0 : src.length;
        SerializationHelper.setVarUint32(buffer, len);
        if (len > 0)
            buffer.put(src);

    }

    @Override
    public byte[] get(ByteBuffer buffer, byte[] holder) throws IOException {
        int len = SerializationHelper.getVarUint32(buffer);
        if (len == 0)
            return null;
        if (holder == null || holder.length != len)
            holder = new byte[len];
        buffer.get(holder);
        return holder;
    }

}

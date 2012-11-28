package org.crunchr.types.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.crunchr.fn.RDoFn;
import org.crunchr.types.RType;
import org.crunchr.types.RTypeState;

/**
 * This is a type to serialize grouped streams of {@link PGroupedTable}.
 * Currently, from java to R only since there's no such thing as grouped
 * emissions so much (I think).
 * <P>
 * 
 * This doesn't serialize the entire group but only just one chunk of it, so
 * {@link RDoFn} need to keep calling it until iterable's hasNext() returns
 * false.
 * 
 * @author dmitriy
 * 
 * @param <K>
 * @param <V>
 */
public class RPGroupedTableType<K, V> extends RType<Pair<K, Iterator<V>>> {

    public static final int MAX_CHUNK_SIZE = 50;

    public static final int FIRST_CHUNK    = 0x01;
    public static final int LAST_CHUNK     = 0x02;

    /* must be less than 2^16 */
    private int             maxChunkSize   = MAX_CHUNK_SIZE;
    private RType<K>        keyRType;
    private RType<V>        valueRType;

    public int getMaxChunkSize() {
        return maxChunkSize;
    }

    public void setMaxChunkSize(int maxChunkSize) {
        this.maxChunkSize = maxChunkSize;
    }

    /**
     * Warning: there's a deviation from a general contract here. This doesn't
     * serialize entire value. Only next chunk of it. Therefore it needs to be
     * called repeatedly until src.second().hasNext()==false.
     */
    @Override
    public void set(ByteBuffer buffer, Pair<K, Iterator<V>> src) throws IOException {

        K key = src.first();

        /* flags */

        int flags = 0;
        int flagsPosition = buffer.position();
        if (key != null)
            flags |= FIRST_CHUNK;

        /* we'll write them later */
        buffer.put((byte) 0);

        /* key */
        if (key != null) {
            keyRType.set(buffer, key);
        }

        /* value count */
        int countPosition = buffer.position();
        int count = 0;
        /*
         * we do not use varint since we'll modify it once we finished writing.
         * we'll write it later.
         */
        buffer.putShort((short) 0);

        /* values */
        Iterator<V> iter = src.second();
        for (count = 0; iter.hasNext() && count < maxChunkSize; count++) {
            V v = iter.next();
            valueRType.set(buffer, v);
        }

        /* flags again */
        if (!iter.hasNext())
            flags |= LAST_CHUNK;
        buffer.put(flagsPosition, (byte) flags);

        /* counter again */

        buffer.putShort(countPosition, (short) count);

    }

    @Override
    public Pair<K, Iterator<V>> get(ByteBuffer buffer, Pair<K, Iterator<V>> holder) throws IOException {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void setSpecificState(ByteBuffer buffer, Configuration conf) throws IOException {
        RTypeStateRType rtypeType = RTypeStateRType.getInstance();
        RTypeState keyState = rtypeType.get(buffer, null);
        RTypeState valueState = rtypeType.get(buffer, null);
        keyRType = RType.fromState(keyState, conf);
        valueRType = RType.fromState(valueState, conf);
        if (keyRType.isMultiEmit() || valueRType.isMultiEmit())
            throw new IOException("Multi-emit types are not accepted for the PGroupedTable serialization");
        multiEmit = keyRType.isMultiEmit();
    }

}

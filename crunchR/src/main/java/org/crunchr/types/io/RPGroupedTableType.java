package org.crunchr.types.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.crunchr.types.RType;
import org.crunchr.types.RTypeState;

/**
 * This is a type to serialize grouped streams of {@link PGroupedTable}.
 * Currently, from java to R only since there's no such thing as grouped
 * emissions so much (I think).
 * <P>
 * 
 * 
 * @author dmitriy
 * 
 * @param <K>
 * @param <V>
 */
public class RPGroupedTableType<K, V> extends RType<Pair<K, Iterable<V>>> {

    public static final int FIRST_CHUNK = 0x01;

    private RType<K>        keyRType;
    private RType<V>        valueRType;

    /**
     * Warning: there's a deviation from a general contract here. This doesn't
     * serialize entire value. Only next chunk of it. Therefore it needs to be
     * called repeatedly until src.second().hasNext()==false.
     */
    @Override
    public void set(ByteBuffer buffer, Pair<K, Iterable<V>> src) throws IOException {

        K key = src.first();

        /* flags */

        int flags = 0;
        int flagsPosition = buffer.position();

        /* we'll write them later */
        buffer.put((byte) 0);

        /* key */
        if (key != null) {
            keyRType.set(buffer, key);
            flags |= FIRST_CHUNK;
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
        Iterator<V> iter = src.second().iterator();
        for (count = 0; iter.hasNext(); count++) {

            V v = iter.next();
            valueRType.set(buffer, v);
        }

        /* flags again */
        buffer.put(flagsPosition, (byte) flags);

        /* counter again */

        buffer.putShort(countPosition, (short) count);

    }

    @Override
    public Pair<K, Iterable<V>> get(ByteBuffer buffer, Pair<K, Iterable<V>> holder) throws IOException {
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

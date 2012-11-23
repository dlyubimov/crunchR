package org.crunchr.types.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.crunchr.types.RType;
import org.crunchr.types.RTypeState;

public class RPTableType<K, V> extends RType<Pair<K, V>> {
    private RType<K> keyRType;
    private RType<V> valueRType;
    private K keyHolder;
    private V valueHolder;

    @Override
    public void set(ByteBuffer buffer, Pair<K, V> src) throws IOException {
        keyRType.set(buffer, src.first());
        valueRType.set(buffer, src.second());
    }

    @Override
    public Pair<K, V> get(ByteBuffer buffer, Pair<K, V> holder) throws IOException {
        keyHolder = keyRType.get(buffer, keyHolder);
        valueHolder = valueRType.get(buffer,valueHolder);
        return Pair.of(keyHolder, valueHolder);
    }

    @Override
    public void setSpecificState(ByteBuffer buffer, Configuration conf) throws IOException {
        RTypeStateRType rtypeType = RTypeStateRType.getInstance();
        RTypeState keyState = rtypeType.get(buffer, null);
        RTypeState valueState = rtypeType.get(buffer, null);
        keyRType = RType.fromState(keyState, conf);
        valueRType = RType.fromState(valueState, conf);
        if (keyRType.isMultiEmit() != valueRType.isMultiEmit())
            throw new IOException("Incompatible key/value R types: both should be multi-emit or not");
        multiEmit = keyRType.isMultiEmit();
    }

}

package org.crunchr.types.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.types.RType;
import org.crunchr.types.RTypeState;

public final class RTypeStateRType extends RType<RTypeState> {

    private static final RTypeStateRType singleton = new RTypeStateRType();

    public static RTypeStateRType getInstance() {
        return singleton;
    }

    @Override
    public void set(ByteBuffer buffer, RTypeState src) throws IOException {
        RString strType = RString.getInstance();
        strType.set(buffer, src.getRClassName());
        strType.set(buffer, src.getJavaClassName());
        RRaw.getInstance().set(buffer, src.getTypeSpecificState());

    }

    @Override
    public RTypeState get(ByteBuffer buffer, RTypeState holder) throws IOException {
        if (holder == null)
            holder = new RTypeState();
        RString strType = RString.getInstance();
        holder.setRClassName(strType.get(buffer, null));
        holder.setJavaClassName(strType.get(buffer, null));
        holder.setTypeSpecificState(RRaw.getInstance().get(buffer, null));
        return holder;
    }

}

package org.crunchr.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.RType;

/**
 * corresponds to character vector in R. 
 * Whoever, we emit each string separately (multiemit = yes). 
 * If a character vector is truly single value then we need to 
 * re-derive and override multiemit = false.
 * @author dmitriy
 *
 */
public class RStrings extends RType<String[]> {

    private static final RStrings singleton = new RStrings();

    public static RStrings getInstance() {
        return singleton;
    }

    @Override
    public void set(ByteBuffer buffer, String[] src) throws IOException {

        SerializationHelper.setVarUint32(buffer, src.length);
        RString rstr = RString.getInstance();
        for (String str : src) {
            rstr.set(buffer, str);
        }
    }

    @Override
    public String[] get(ByteBuffer buffer, String[] holder) throws IOException {
        int count = SerializationHelper.getVarUint32(buffer);
        String[] result = new String[count];
        RString rstr = RString.getInstance();
        for (int i = 0; i < count; i++) {
            result[i] = rstr.get(buffer, null);
        }
        return result;
    }

    @Override
    public boolean isMultiEmit() {
        return true;
    }
    
    

}

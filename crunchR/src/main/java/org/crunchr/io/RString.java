package org.crunchr.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.RType;

public class RString extends RType<String> {
    
    private static final RString singleton = new RString();
    
    public static RString getInstance() { return singleton; }

	@Override
	public void set(ByteBuffer buffer, String src) throws IOException {
		byte[] bytesU8 = src.getBytes("utf-8");
		int len = bytesU8.length;
		if (len != len << -Short.SIZE >>> -Short.SIZE)
			throw new IOException("String too long");
		SerializationHelper.setVarUint32(buffer, len);
		buffer.put(bytesU8);
	}

	@Override
	public String get(ByteBuffer buffer, String holder) throws IOException {
	    int len = SerializationHelper.getVarUint32(buffer);
		String str = new String(buffer.array(), buffer.position(), len, "utf-8");
		buffer.position(buffer.position() + len);
		return str;

	}

}

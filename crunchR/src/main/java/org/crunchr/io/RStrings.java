package org.crunchr.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.crunchr.RType;

public class RStrings implements RType<String[]> {

	private RString strType = new RString();

	@Override
	public void set(ByteBuffer buffer, String[] src) throws IOException {
		SerializationHelper.setVarUint32(buffer, src.length);
		for (String str : src) {
			strType.set(buffer, str);
		}
	}

	@Override
	public String[] get(ByteBuffer buffer, String[] holder) throws IOException {
		int count = SerializationHelper.getVarUint32(buffer);
		String[] result = new String[count];
		for (int i = 0; i < count; i++) {
			result[i] = strType.get(buffer, null);
		}
		return result;
	}

}

package org.crunchr.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SerializationHelper {
	public static void setVarUint32(ByteBuffer bb, int val) throws IOException {
		for (;; val >>>= 7) {
			if ((val & ~0x7f) == 0) {
				bb.put((byte) val);
				break;
			} else
				bb.put((byte) (0x80 | val));
		}
	}

	public static int getVarUint32(ByteBuffer bb) throws IOException {
		int accum = 0, bitsRead = 0;
		do {
			int c = bb.get() & 0xff;

			if ((c & 0x80) == 0)
				return accum | c << bitsRead;
			else
				accum |= (c & 0x7f) << bitsRead;
			bitsRead += 7;
		} while (bitsRead < 35);
		throw new IOException("Illegal Varint format");
	}

}

package org.crunchr.io;

import java.nio.ByteBuffer;

import org.crunchr.types.io.SerializationHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerializationHelperTest {
    
    public int testVarUint32(int k) throws Exception { 
        ByteBuffer buff = ByteBuffer.allocate(5);
        SerializationHelper.setVarUint32(buff, k);
        buff.flip();
        Assert.assertEquals(SerializationHelper.getVarUint32(buff),k);
        return buff.position();
    }

    public int testVarInt32(int k) throws Exception { 
        ByteBuffer buff = ByteBuffer.allocate(5);
        SerializationHelper.setVarInt32(buff, k);
        buff.flip();
        Assert.assertEquals(SerializationHelper.getVarInt32(buff),k);
        return buff.position();
    }
    
    @Test
    public void varTests() throws Exception { 
        int[] vals = new int[] { 0,1,-1,Integer.MAX_VALUE,Integer.MIN_VALUE,20,30};
        int[] spaceInt32 = new int[] { 1,1,1,5,5,1,1};
        int[] spaceUint32 = new int[] { 1,1,5,5,5,1,1};
        
        for (int i = 0; i < vals.length; i++ ) { 
            Assert.assertEquals(spaceInt32[i], testVarInt32(vals[i]));
            Assert.assertEquals(spaceUint32[i], testVarUint32(vals[i]));
        }
    }

}

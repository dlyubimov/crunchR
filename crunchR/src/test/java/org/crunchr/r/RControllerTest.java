package org.crunchr.r;

import org.crunchr.io.TwoWayRPipe;
import org.testng.annotations.Test;

public class RControllerTest {
    
    
    @Test
    public void test1() throws Exception { 
        RController rController = RController.getInstance(null);
        TwoWayRPipe rpipe = rController.getRPipe();
        rpipe.start();
        rpipe.shutdown(true);
        
    }

}

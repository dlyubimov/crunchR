package example1;

import org.apache.crunch.impl.mr.MRPipeline;
import org.testng.annotations.Test;

public class Scratchpad {

    @Test
    public void pipelineTest() throws Exception { 
        MRPipeline pipeline = new MRPipeline(Scratchpad.class,"scratchpad1");
        
    }
}

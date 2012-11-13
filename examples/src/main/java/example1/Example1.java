package example1;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;

/**
 * Placeholder ... 
 * 
 * @author dmitriy
 *
 */
public class Example1 {
	
	public static void main(String[] args){ 
		Pipeline pl = new MRPipeline(Example1.class);
		PCollection<String> lines = pl.readTextFile(args[0]);
	}

}

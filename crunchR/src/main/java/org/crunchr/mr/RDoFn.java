package org.crunchr.mr;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public class RDoFn<S, T> extends DoFn<S, T> {

	private static final long serialVersionUID = 1L;
	
	private byte[] rinit,rprocess,rcleanup;

	public byte[] getRinit() {
		return rinit;
	}

	public void setRinit(byte[] rinit) {
		this.rinit = rinit;
	}

	public byte[] getRprocess() {
		return rprocess;
	}

	public void setRprocess(byte[] rprocess) {
		this.rprocess = rprocess;
	}

	public byte[] getRcleanup() {
		return rcleanup;
	}

	public void setRcleanup(byte[] rcleanup) {
		this.rcleanup = rcleanup;
	}

	@Override
	public void cleanup(Emitter<T> emitter) {
		// TODO Auto-generated method stub
		super.cleanup(emitter);
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		super.initialize();
	}



	@Override
	public void process(S arg0, Emitter<T> arg1) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	

}

package org.crunchr.types;

import java.io.Serializable;

public final class RTypeState implements Serializable {

    private static final long serialVersionUID = 1L;

    private String            rClassName, javaClassName;
    private byte[]            typeSpecificState;

    public String getRClassName() {
        return rClassName;
    }

    public void setRClassName(String rClassName) {
        this.rClassName = rClassName;
    }

    public String getJavaClassName() {
        return javaClassName;
    }

    public void setJavaClassName(String javaClassName) {
        this.javaClassName = javaClassName;
    }

    public byte[] getTypeSpecificState() {
        return typeSpecificState;
    }

    public void setTypeSpecificState(byte[] typeSpecificState) {
        this.typeSpecificState = typeSpecificState;
    }

}

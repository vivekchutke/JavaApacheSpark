package com.vivekchutke.javaspark.apachespark;


public class TupleBeanObject {
    private Double value1;
    private Double valueSqrRoot;

    public TupleBeanObject(Double value){
        this.value1 = value;
        this.valueSqrRoot = Math.sqrt(value);
    }

    public Double getValue1() {
        return value1;
    }

    public void setValue1(Double value1) {
        this.value1 = value1;
    }

    public Double getValueSqrRoot() {
        return valueSqrRoot;
    }

    public void setValueSqrRoot(Double valueSqrRoot) {
        this.valueSqrRoot = valueSqrRoot;
    }
}

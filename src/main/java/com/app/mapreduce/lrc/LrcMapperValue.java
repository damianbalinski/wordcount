package com.app.mapreduce.lrc;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LrcMapperValue implements Writable, Cloneable {
    private String symbol;
    private long timestamp;
    private double price;

    @Override
    public LrcMapperValue clone() throws CloneNotSupportedException {
        return (LrcMapperValue) super.clone();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(getSymbol());
        dataOutput.writeLong(getTimestamp());
        dataOutput.writeDouble(getPrice());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setSymbol(dataInput.readUTF());
        setTimestamp(dataInput.readLong());
        setPrice(dataInput.readDouble());
    }

    @Override
    public String toString() {
        return "Value(symbol="+ getSymbol() + ", timestamp=" + getTimestamp() + ", price=" + getPrice() + ")";
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}

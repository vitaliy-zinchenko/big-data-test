package zinjvi;

import java.io.Serializable;

public class LogData implements Serializable{

    private long bytes;
    private int number;

    public LogData() {
    }

    public LogData(long bytes, int number) {
        this.bytes = bytes;
        this.number = number;
    }

    @Override
    public String toString() {
        return bytes + "," + number;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}

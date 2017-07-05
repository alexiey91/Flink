package main.java.it.valenti.salome.flink.util;

/**
 * Created by root on 24/06/17.
 */
public class Query1Object {
    private String timestamp;
    private int x;
    private int y;
    private int z;
    private int speed;

    public Query1Object(){};

    public Query1Object(String timestamp, int x, int y, int z, int speed) {
        this.timestamp = timestamp;
        this.x = x;
        this.y = y;
        this.z = z;
        this.speed = speed;
    }



    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getZ() {
        return z;
    }

    public void setZ(int z) {
        this.z = z;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }
}

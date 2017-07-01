package main.java.it.valenti.salome.flink.util;

/**
 * Created by Paolo on 01/07/2017.
 */
public class TupleTreeSet implements Comparable<TupleTreeSet> {

    private String id;
    private double vel;


    public TupleTreeSet( String id,double vel) {
        this.id = id;
        this.vel = vel;
    }

    public double getVel() {
        return vel;
    }

    public void setVel(double vel) {
        this.vel = vel;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
        public int compareTo(TupleTreeSet emp) {
            if (this.vel >= emp.getVel()) {
                return -1;
            } /*else if (this.vel > emp.getVel()) {
                return -1;
            }*/
            return 1;
        }
    }

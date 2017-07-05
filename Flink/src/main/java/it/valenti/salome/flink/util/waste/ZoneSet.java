package main.java.it.valenti.salome.flink.util;

import java.util.Set;

/**
 * Created by Paolo on 01/07/2017.
 */
public class ZoneSet implements Comparable<ZoneSet> {

    private String id;
    private long cnt;


    public ZoneSet(String id, long cnt) {
        this.id = id;
        this.cnt = cnt;
    }

    public long getCnt() {
        return cnt;
    }

    public void setCnt(long cnt) {
        this.cnt = cnt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public int compareTo(ZoneSet emp) {
        if (this.cnt == emp.getCnt()) {
            return 0;
        } else if (this.cnt > emp.getCnt()) {
                return -1;
            }
        return 1;
    }
    /*public static long getSetValue(Set<ZoneSet> set, String value){
        for (ZoneSet i: set)
            if(i.getId().equals(value))
                return i.getcnt();
        return 0;
    }*/
}

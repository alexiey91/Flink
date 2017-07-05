package main.java.it.valenti.salome.flink.util;

import scala.annotation.serializable;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by Paolo on 01/07/2017.
 */

public class ZoneMap implements Serializable {
    private HashMap<String,Set<ZoneSet>> map ;
    private String name;

    public ZoneMap() {
        this.map = new HashMap<>();
    }

    public HashMap<String, Set<ZoneSet>> getMap() {
        return map;
    }

    public void setMap(HashMap<String, Set<ZoneSet>> map) {
        this.map = map;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void updateMap(String name, String zone, long value){


        if(!map.containsKey(name)){
            Set<ZoneSet> treeSet = new TreeSet<>();
            treeSet.add(new ZoneSet(zone,value));
            map.put(name,treeSet);
        }else{
            map.get(name).add(new ZoneSet(zone,value));
        }
    }
    public void eraseMap(){
        this.map = new HashMap<>();
    }

}



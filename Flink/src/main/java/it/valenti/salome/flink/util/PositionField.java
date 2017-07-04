package main.java.it.valenti.salome.flink.util;

import java.text.DecimalFormat;

/**
 * Created by root on 02/07/17.
 */
public class PositionField {
    private double X_lenght= 52.483;
    private double Y_lenght = 67.925;
    private  double x_cell = 6.5604;
    private double y_cell = 5.225;

    //traslo la y della metà di Y_lenght
    // converto le x in abs se negative
    public PositionField() {
    }

    public String positionCell(Double x, Double y){
    String cella="";
    if(x <0) Math.abs(x);
    //valore per il calcolo della posizione della cella in y risperro il vertice in basso a sinistra
    double adjustY= (Y_lenght/2)+y;
    double cellX = Math.round(x/x_cell);
    double cellY = Math.round(adjustY/y_cell);
        DecimalFormat format = new DecimalFormat();
    cella="("+format.format(cellX)+"-"+format.format(cellY)+")";
    return cella;}

}

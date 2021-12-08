package com.bigdata.springboot.model;

import org.springframework.stereotype.Component;

public class GridPower {

    private int X;

    private int Y;

    private int power;

    //private boolean selected;

    public GridPower(GridPower gridPower) {         //clone constructor
        this.X = gridPower.getX();
        this.Y = gridPower.getY();
        this.power = gridPower.getPower();
        //this.selected = gridPower.isSelected();
    }

   public GridPower(int x, int y, int power) {
        X = x;
        Y = y;
        this.power = power;
        //this.selected = false;
    }

    /*public GridPower(int x, int y, int power) {
        X = x;
        Y = y;
        this.power = power;
        //this.selected = selected;
    }*/

    public int getX() {
        return X;
    }

    public void setX(int x) {
        X = x;
    }

    public int getY() {
        return Y;
    }

    public void setY(int y) {
        Y = y;
    }

    public int getPower() {
        return power;
    }

    public void setPower(int power) {
        this.power = power;
    }

    public void offsetXY( int offsetX, int offsetY) {
        X += offsetX;
        Y += offsetY;
    }
    /*public boolean isSelected() {
        return selected;
    }

    public void setSelected(boolean selected) {
        this.selected = selected;
    }*/
}

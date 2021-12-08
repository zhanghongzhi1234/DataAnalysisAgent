package com.bigdata.springboot.model;

import org.springframework.stereotype.Component;

@Component
public class LocationModel {

    private String powerArray;      //all grid power seperate by comma, unit is MW, left top is original point, row by row

    private int turbineNumber;     //2MW or 3MW wind trap number

    //private int turbineDiameter;   //wind tuebine diameter

    private int spaceXInMeters;     //wind turbine X minimum space

    private int spaceYInMeters;     //wind turbine X minimum space

    private int gridX;             //grid width: 198

    private int gridY;             //grid height: 134

    private int gridUnit;          //50m

    public String getPowerArray() {
        return powerArray;
    }

    public void setPowerArray(String powerArray) {
        this.powerArray = powerArray;
    }

    public int getTurbineNumber() {
        return turbineNumber;
    }

    public void setTurbineNumber(int turbineNumber) {
        this.turbineNumber = turbineNumber;
    }

    public int getSpaceXInMeters() {
        return spaceXInMeters;
    }

    public void setSpaceXInMeters(int spaceXInMeters) {
        this.spaceXInMeters = spaceXInMeters;
    }

    public int getSpaceYInMeters() {
        return spaceYInMeters;
    }

    public void setSpaceYInMeters(int spaceYInMeters) {
        this.spaceYInMeters = spaceYInMeters;
    }

    public int getGridX() {
        return gridX;
    }

    public void setGridX(int gridX) {
        this.gridX = gridX;
    }

    public int getGridY() {
        return gridY;
    }

    public void setGridY(int gridY) {
        this.gridY = gridY;
    }

    public int getGridUnit() {
        return gridUnit;
    }

    public void setGridUnit(int gridUnit) {
        this.gridUnit = gridUnit;
    }
}

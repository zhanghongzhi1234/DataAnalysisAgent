package com.bigdata.springboot.controller;

import com.bigdata.springboot.bean.CommonUtil;
import com.bigdata.springboot.bean.ResponseBean;
import com.bigdata.springboot.model.GridPower;
import com.bigdata.springboot.model.LocationModel;
import com.bigdata.springboot.model.QueryModel;
import com.bigdata.springboot.service.DataAnalysisService;
import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class WebController {

    @Autowired
    private DataAnalysisService dataAnalysisService;

    private Gson gson = new Gson();

    @PostMapping("/api/wind/max")
    public Object Max(@RequestBody QueryModel model) {
        //check request data format first
        if (model == null)
            return (new ResponseBean(400, "invalid json body object")).getData();

        String start_time = model.getStart_time();
        String end_time = model.getEnd_time();
        long entitykey = model.getEntitykey();
        if ( entitykey < 0 )
            return (new ResponseBean(400, "entitykey must be an positive number")).getData();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date dateStart = dateFormat.parse(start_time);
            Date dateEnd = dateFormat.parse(end_time);
            if ( entitykey < 0 || dateStart.after(dateEnd) )
                return (new ResponseBean(400, "start time cannot be after end time")).getData();
        } catch (ParseException e) {
            e.printStackTrace();
            return (new ResponseBean(400, "invalid json body object")).getData();
        }
        double value = dataAnalysisService.max(start_time, end_time, entitykey);

        Map<String, String> data = new HashMap<String, String>() {{  put("value", String.valueOf(value)); }};
        return new ResponseBean(200, "success", data).getData();
    }

    @PostMapping("/api/wind/min")
    public Object Min(@RequestBody QueryModel model) {
        //check request data format first
        if (model == null)
            return (new ResponseBean(400, "invalid json body object")).getData();

        String start_time = model.getStart_time();
        String end_time = model.getEnd_time();
        long entitykey = model.getEntitykey();
        if ( entitykey < 0 )
            return (new ResponseBean(400, "entitykey must be an positive number")).getData();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date dateStart = dateFormat.parse(start_time);
            Date dateEnd = dateFormat.parse(end_time);
            if ( entitykey < 0 || dateStart.after(dateEnd) )
                return (new ResponseBean(400, "start time cannot be after end time")).getData();
        } catch (ParseException e) {
            e.printStackTrace();
            return (new ResponseBean(400, "invalid json body object")).getData();
        }
        double value = dataAnalysisService.min(start_time, end_time, entitykey);

        Map<String, String> data = new HashMap<String, String>() {{  put("value", String.valueOf(value)); }};
        return new ResponseBean(200, "success", data).getData();
    }

    @PostMapping("/api/wind/ave")
    public Object Average(@RequestBody QueryModel model) {
        //check request data format first
        if (model == null)
            return (new ResponseBean(400, "invalid json body object")).getData();

        String start_time = model.getStart_time();
        String end_time = model.getEnd_time();
        long entitykey = model.getEntitykey();
        if ( entitykey < 0 )
            return (new ResponseBean(400, "entitykey must be an positive number")).getData();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date dateStart = dateFormat.parse(start_time);
            Date dateEnd = dateFormat.parse(end_time);
            if ( entitykey < 0 || dateStart.after(dateEnd) )
                return (new ResponseBean(400, "start time cannot be after end time")).getData();
        } catch (ParseException e) {
            e.printStackTrace();
            return (new ResponseBean(400, "invalid json body object")).getData();
        }
        double value = dataAnalysisService.average(start_time, end_time, entitykey);

        Map<String, String> data = new HashMap<String, String>() {{  put("value", String.valueOf(value)); }};
        return new ResponseBean(200, "success", data).getData();
    }

    @PostMapping("/api/wind/location")
    @CrossOrigin(origins = "*")
    public Object Average(@RequestBody LocationModel model) {
        //check request data format first
        if (model == null)
            return (new ResponseBean(400, "invalid json body object")).getData();
        String powerArray = model.getPowerArray();
        int turbineNumber = model.getTurbineNumber();      //2MW or 3MW wind trap number
        //int turbineDiameter = model.getTurbineDiameter();  //wind tuebine diameter, no need
        int spaceXInMeters = model.getSpaceXInMeters();      //wind turbine X minimum space
        int spaceYInMeters = model.getSpaceYInMeters();      //wind turbine X minimum space
        int gridX = model.getGridX();                      //grid width: 198
        int gridY = model.getGridY();                      //grid height: 134
        int gridUnit = model.getGridUnit();                //50m
        if ( turbineNumber <= 0 || spaceXInMeters <= 0 || spaceYInMeters <= 0 || gridX <= 0 || gridY <= 0 || gridUnit <= 0)
            return (new ResponseBean(400, "invalid json body object")).getData();

        List<List<GridPower>> value = dataAnalysisService.calTurbineLocation(powerArray, turbineNumber, spaceXInMeters, spaceYInMeters, gridX, gridY, gridUnit);

        Map<String, String> data = new HashMap<String, String>() {{  put("value", gson.toJson(value)); }};
        return new ResponseBean(200, "success", data).getData();
    }

    @PostMapping("/api/wind/startrollup")
    @CrossOrigin(origins = "*")
    public Object StartRollup() {
        dataAnalysisService.startRollup();
        return new ResponseBean(200, "success").getData();
    }

    @PostMapping("/api/wind/stoprollup")
    @CrossOrigin(origins = "*")
    public Object StopRollup() {
        dataAnalysisService.stopRollup();
        return new ResponseBean(200, "success").getData();
    }

    @PostMapping("/api/wind/group")
    @CrossOrigin(origins = "*")
    public Object Group(@RequestBody QueryModel model) {
        //check request data format first
        if (model == null)
            return (new ResponseBean(400, "invalid json body object")).getData();

        String start_time = model.getStart_time();
        String end_time = model.getEnd_time();
        /*long entitykey = model.getEntitykey();
        if ( entitykey < 0 )
            return (new ResponseBean(400, "entitykey must be an positive number")).getData();*/

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date dateStart = dateFormat.parse(start_time);
            Date dateEnd = dateFormat.parse(end_time);
            if ( dateStart.after(dateEnd) )
                return (new ResponseBean(400, "start time cannot be after end time")).getData();
        } catch (ParseException e) {
            e.printStackTrace();
            return (new ResponseBean(400, "invalid json body object")).getData();
        }
        double value = dataAnalysisService.groupByEntity(start_time, end_time);

        Map<String, String> data = new HashMap<String, String>() {{  put("value", String.valueOf(value)); }};
        return new ResponseBean(200, "success", data).getData();
    }
}

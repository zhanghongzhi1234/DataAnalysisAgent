package com.bigdata.springboot.model;

import org.springframework.stereotype.Component;

@Component
public class QueryModel {

    private String start_time;

    private String end_time;

    private long entitykey;

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public long getEntitykey() {
        return entitykey;
    }

    public void setEntitykey(long entitykey) {
        this.entitykey = entitykey;
    }
}

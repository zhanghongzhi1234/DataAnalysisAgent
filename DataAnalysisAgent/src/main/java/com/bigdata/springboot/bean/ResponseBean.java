package com.bigdata.springboot.bean;

import java.util.HashMap;
import java.util.Map;

public class ResponseBean {

    private int code;

    private String msg;

    private Map<String, String> data = new HashMap<String, String>();
    ;

    public ResponseBean(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public ResponseBean(int code, String msg, Map<String, String> data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public Object getData() {

        String status = "success";
        switch (code) {
            case 200:
                status = "success";
                break;
            case 400:
            case 401:
                status = "failed";
                break;
        }
        Map<String, String> mRet = new HashMap<String, String>();
        mRet.put("status_code", Integer.toString(code));
        mRet.put("status", status);
        mRet.put("msg", msg);
        mRet.putAll(data);

        return mRet;
    }
}

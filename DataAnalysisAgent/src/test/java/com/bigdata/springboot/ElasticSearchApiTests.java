package com.bigdata.springboot;

import com.bigdata.springboot.helper.ElasticSearchHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class ElasticSearchApiTests {

    private String host = "192.168.1.104";
    private int port1 = 9200;
    private int port2 = 9201;
    private int archiveKeepDays = 7;
    private String[] orginal_indices = {"windspeed1"};
    private String archive_index = "windspeed_archive";

    private static final Logger logger = LogManager.getLogger();

    private ElasticSearchHelper esHelper = null;

    @BeforeClass
    public void Setup() {
        try {
            esHelper = new ElasticSearchHelper(host, port1, port2);
        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
        }
    }

    @AfterClass
    public void Close() {
        esHelper.Close();
        esHelper = null;
    }

    @Test(groups = "groupCorrect")
    public void test1() {
        {
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            Date end_time = cal.getTime();
            System.out.println(end_time);
        }
        {
            Calendar cal = Calendar.getInstance();      //each time getInstance will new one, it is not singleton model in C++
            Date new_time = cal.getTime();
            System.out.println(new_time);
        }
    }

    @Test(groups = "groupCorrect")
    private void testDeleteOutdatedRecordInArchiveIndex() {
        System.out.println("Try to delete outdated record..");
        //every midnight check db and delete archive max than archiveKeepDays
        LocalDate today = LocalDate.now();
        LocalDate archiveDeadline = today.minusDays(archiveKeepDays);
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");         //it will crash when format due to LocalDate have no hour, minute and second field
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String endTime = archiveDeadline.format(formatter) + " 00:00:00";
        //String endTime = archiveDeadline.format(formatter);                   //it will delete wrong data if not use exactly time format
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("timestamp").lt(endTime);

        long count = esHelper.DeleteDataByQuery(rangeQueryBuilder, "windspeed1");
        System.out.println("Delete data exceed " + archiveKeepDays + " days from index, total delete " + count + " docs");
    }
}

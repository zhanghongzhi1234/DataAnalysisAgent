package com.bigdata.springboot.bean;

import com.bigdata.springboot.helper.ElasticSearchHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class RollupTask extends TimerTask {

    private ElasticSearchHelper esHelper = null;
    private int intervalMinutes;
    private int rollupKeepDays;
    private int archiveKeepDays;
    private String[] orginal_indices;     //can be multiple index seperate by ','
    private String rollup_index;
    private String archive_index;
    private boolean deleteAllOnStart;
    private int previousDay;
    private boolean firstStart = true;
    private boolean stillInProcess = false;
    private static final Logger logger = LogManager.getLogger();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date lastExecuteStartTime = null;
    //private boolean rollupTooLong = false;                  //when next execute cycle come, last rollup still not finish

    public RollupTask(){}

    public RollupTask(ElasticSearchHelper esHelper, int intervalMinutes, int rollupKeepDays, int archiveKeepDays, String orginal_indices, String rollup_index, String archive_index, boolean deleteAllOnStart) {
        init(esHelper, intervalMinutes, rollupKeepDays, archiveKeepDays, orginal_indices, rollup_index, archive_index, deleteAllOnStart);
    }

    public void init(ElasticSearchHelper esHelper, int intervalMinutes, int rollupKeepDays, int archiveKeepDays, String orginal_indices, String rollup_index, String archive_index, boolean deleteAllOnStart) {
        this.esHelper = esHelper;
        this.intervalMinutes = intervalMinutes;
        this.rollupKeepDays = rollupKeepDays;
        this.archiveKeepDays = archiveKeepDays;
        this.orginal_indices = orginal_indices.split(",");
        this.rollup_index = rollup_index;
        this.archive_index = archive_index;
        this.deleteAllOnStart = deleteAllOnStart;
        previousDay = Calendar.getInstance().get(Calendar.DATE);
    }
    /**
     * The action to be performed by this timer task. evecute every minute, java timer is single thread, so won't trigger twice if last timer cycle not finished
     */
    @Override
    public void run() {
        if(stillInProcess) {
            return;
        }
        stillInProcess = true;

        Calendar cal = Calendar.getInstance();
        int currentDay = cal.get(Calendar.DATE);
        if (firstStart || currentDay != previousDay) {
            System.out.println("Delete outdated data when first time start or a new day start");
            int maxKeepDays = Math.max(rollupKeepDays, archiveKeepDays);
            deleteOutdatedRecord(maxKeepDays, orginal_indices);
            deleteOutdatedRecord(rollupKeepDays, rollup_index);
            deleteOutdatedRecord(archiveKeepDays, archive_index);
            previousDay = currentDay;
        }
        {
            boolean bRollupTooLongLastTime = false;
            if(!firstStart) {
                Calendar calNext = Calendar.getInstance();
                calNext.setTime(lastExecuteStartTime);
                calNext.add(Calendar.MINUTE, intervalMinutes);
                if(calNext.getTime().before(cal.getTime())) {       //next execute time pass already due to last time execute too long
                    bRollupTooLongLastTime = true;
                }
            }
            lastExecuteStartTime = cal.getTime();
            if(firstStart)
                System.out.println("First time start, check if have history data that exceed 1 rollup cycle");
            if(bRollupTooLongLastTime)
                System.out.println("Last rollup take too long time, check if have new data that exceed 1 rollup cycle");
            if (firstStart || bRollupTooLongLastTime) {
                // roll up all data when first start
                rollupAllHistoryData();
                firstStart = false;
            }
        }

        //Calendar cal = Calendar.getInstance();        //can use previous cal
        int minutes = cal.get(Calendar.HOUR) * 60 + cal.get(Calendar.MINUTE);
        if(minutes % intervalMinutes == 0) {
            // All below operation will add term filter from start_time to end_time
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            Date endTime = cal.getTime();
            cal.add(Calendar.MINUTE, -intervalMinutes);
            Date startTime = cal.getTime();

            rollupDataBetween(startTime, endTime);
        }

        stillInProcess = false;
    }

    private void deleteOutdatedRecord(int keepDays, String... indices) {
        System.out.println("Try to delete outdated record..");
        //every midnight check db and delete archive max than archiveKeepDays
        LocalDate today = LocalDate.now();
        LocalDate archiveDeadline = today.minusDays(keepDays);
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");     //it will crash when format due to LocalDate have no hour, minute and second field
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        //String endTime = archiveDeadline.format(formatter);                   //it will delete wrong data if not use exactly time format
        String endTime = archiveDeadline.format(formatter) + " 00:00:00";
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("timestamp").lt(endTime);

        long count = esHelper.DeleteDataByQuery(rangeQueryBuilder, indices);
        System.out.println("Delete data exceed " + archiveKeepDays + " days from index: " + String.join(",", indices) + ", total delete " + count + " docs");
    }

    //rollup all history data before last intervalMinutes timepoint
    private void rollupAllHistoryData() {
        System.out.println("Rollup all history data...");
        String minTimeString = getMinTimeString(orginal_indices);     //for testing purpose
        if(minTimeString == "Infinity") {
            System.out.println("There are no data in original_indices, no need rollup");
            return;
        }
        try {
            Date minTime = sdf.parse(minTimeString);
            Calendar cal1 = Calendar.getInstance();
            cal1.setTime(minTime);
            cal1.set(Calendar.SECOND, 0);
            cal1.set(Calendar.MILLISECOND, 0);
            int minute = cal1.get(Calendar.MINUTE);
            cal1.set(Calendar.MINUTE, (minute / intervalMinutes) * intervalMinutes);
            Date startTime = cal1.getTime();

            Calendar cal2 = Calendar.getInstance();
            cal2.set(Calendar.SECOND, 0);
            cal2.set(Calendar.MILLISECOND, 0);
            minute = cal2.get(Calendar.MINUTE);
            cal2.set(Calendar.MINUTE, (minute / intervalMinutes) * intervalMinutes);
            Date endTime = cal2.getTime();

            if(startTime.before(endTime)) {
                rollupDataBetween(startTime, endTime);
            }
            else {
                System.out.println("Min timestamp is within 1 rolluup cycle,  no history data to be rollup");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    //rollup data between startTime and endtime, both time must be multiple of intervalMinutes, internal function
    private void rollupDataBetween(Date startTime, Date endTime) {
        System.out.println("Rollup data between startTime:" + sdf.format(startTime) + ", endTime:" + sdf.format(endTime));
        if(startTime.before(endTime)) {

            // 1. roll up all data from startTime to endTime by intervalMinutes
            Calendar cal1 = Calendar.getInstance();
            cal1.setTime(startTime);
            Date itStartTime = startTime;
            cal1.add(Calendar.MINUTE, intervalMinutes);
            Date itEndTime = cal1.getTime();
            while (!itEndTime.after(endTime)) {     //execute when itEndTime <= endTime
                //BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();                //use BoolQueryBuilder in future if have more condition
                //boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(sdf.format(startTime)).lt(sdf.format(endTime)));          //filter same with must but not count into score, it is fastest
                System.out.println("Rollup record startTime:" + sdf.format(itStartTime) + ", endTime:" + sdf.format(itEndTime));
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("timestamp").gte(sdf.format(itStartTime)).lt(sdf.format(itEndTime));
                aggregateDataToRollupIndex(rangeQueryBuilder, sdf.format(itStartTime));     //roll up all data from original indices to rollupindex
                itStartTime = itEndTime;
                cal1.add(Calendar.MINUTE, intervalMinutes);
                itEndTime = cal1.getTime();
            }

            //2. reindex all data from original indices to achiveindex
            System.out.println("Start to reindex all data from original indices to achiveindex, startTime:" + sdf.format(startTime) + ", endTime:" + sdf.format(endTime));
            RangeQueryBuilder allRangeQueryBuilder = QueryBuilders.rangeQuery("timestamp").gte(sdf.format(startTime)).lt(sdf.format(endTime));
            long reIndexCount = esHelper.reIndex(allRangeQueryBuilder, archive_index, orginal_indices);
            System.out.println("Rollup record startTime:" + sdf.format(itStartTime) + ", endTime:" + sdf.format(itEndTime));
            System.out.println("reIndex " + reIndexCount + " docs to archive_index");

            //3. delete old data in original indices to optimize rollup speed
            System.out.println("Start to delete data from original index...");
            long delCount = esHelper.DeleteDataByQuery(allRangeQueryBuilder, orginal_indices);
            System.out.println("Delete data from original index, total delete " + delCount + " docs");
        }
        else {
            System.out.println("startTime is after endtime,  no data will be rollup");
        }


    }

    private void aggregateDataToRollupIndex(QueryBuilder queryBuilder, String timestamp) {
        System.out.println("Aggregate data to rollup index..., timestamp: " + timestamp);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if(queryBuilder != null)
            searchSourceBuilder.query(queryBuilder);

        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(0);
        String agg_name = "agg_by_entitykey";
        TermsAggregationBuilder aggregation = AggregationBuilders.terms(agg_name).field("entitykey");
        String subagg_name = "stats_value";
        AggregationBuilder subAggr = AggregationBuilders.stats(subagg_name).field("value");
        aggregation.subAggregation(subAggr);
        searchSourceBuilder.aggregation(aggregation);

        Aggregations aggs = esHelper.GetAggregationBySearch(searchSourceBuilder, orginal_indices);
        if(aggs != null) {
            Terms byEntityAggregation = aggs.get(agg_name);
            List<? extends Terms.Bucket> buckets = byEntityAggregation.getBuckets();
            System.out.println("aggregation by entitykey, total bucket: " + buckets.size());
            List<Map<String, Object>> dataMapList = new ArrayList<Map<String, Object>>();
            for (Terms.Bucket bucket : buckets) {
                Stats stats = bucket.getAggregations().get(subagg_name);
                System.out.println(bucket.getKey()); //获取分组名称
                System.out.println("stats：" + stats.toString());
                Map<String, Object> dataMap = new HashMap<String, Object>();
                dataMap.put("entitykey", bucket.getKey().toString());
                dataMap.put("max_value", stats.getMaxAsString());
                dataMap.put("min_value", stats.getMinAsString());
                dataMap.put("avg_value", stats.getAvgAsString());
                dataMap.put("timestamp", timestamp);
                dataMapList.add(dataMap);
            }
            boolean ret = esHelper.BulkyInsertData(rollup_index, dataMapList, true);
            dataMapList.clear();
            dataMapList = null;
        }
    }

    private String getMinTimeString(String... indices) {
        String ret = "";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(0);
        String agg_name = "min_time";
        AggregationBuilder aggr = AggregationBuilders.min(agg_name).field("timestamp");
        searchSourceBuilder.aggregation(aggr);

        Aggregations aggs = esHelper.GetAggregationBySearch(searchSourceBuilder, indices);
        if(aggs != null) {
            Min min = aggs.get(agg_name);
            System.out.println("min_time: " + min.getValueAsString());
            ret = min.getValueAsString();
        }

        return ret;
    }
}

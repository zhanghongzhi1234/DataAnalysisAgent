package com.bigdata.springboot.service;

import com.bigdata.springboot.bean.RollupTask;
import com.bigdata.springboot.helper.ElasticSearchHelper;
//import io.jsonwebtoken.Claims;
import com.bigdata.springboot.model.GridPower;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class DataAnalysisService implements InitializingBean {

    @Value("${esserver.host}")
    private String esserver_host;

    @Value("${esserver.port1}")
    private int esserver_port1;

    @Value("${esserver.port2}")
    private int esserver_port2;

    @Value("${esserver.index}")
    private String esserver_index;

    @Value("${rollup.orginalindices}")
    private String orginal_indices;

    @Value("${rollup.rollupindex}")
    private String rollup_index;

    @Value("${rollup.archiveindex}")
    private String archive_index;

    @Value("${rollup.intervalminutes}")
    private int rollup_intervalminutes;

    @Value("${rollup.archivekeepdays}")
    private int rollup_archivekeepdays;

    @Value("${rollup.rollupkeepdays}")
    private int rollup_rollupkeepdays;

    @Value("${rollup.deleteallonstart}")
    private boolean deleteAllOnStart;

    private static final Logger logger = LogManager.getLogger();

    private ElasticSearchHelper esHelper = null;

    private Timer timer;

    private RollupTask rollTask;

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            esHelper = new ElasticSearchHelper(esserver_host, esserver_port1, esserver_port2);
            if (esHelper == null) {
                System.out.println("Cannot open create elastic search helper at " + esserver_host + ":" + esserver_port1 + "," + esserver_port2 + ", program will exit");
                System.exit(-1);
            }
            if(!esHelper.ping()) {
                System.out.println("Cannot connect to elastic search node at " + esserver_host + ":" + esserver_port1 + "," + esserver_port2 + ", program will exit");
                System.exit(-1);
            }
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }

        //List<String> indexList = Arrays.asList(orginal_indices.split(","));       //will crash if use this indexList to add
        List<String> indexList = new ArrayList<>();
        indexList.addAll(Arrays.asList(orginal_indices.split(",")));
        indexList.add(rollup_index);
        indexList.add(archive_index);
        for (String index : indexList) {
            if(!esHelper.IndexExist(index)) {
                System.out.println("index: " + index + " not exist, program will exit");
                logger.error("index: " + index + " not exist, program will exit");
                System.exit(-1);
            }
        }
    }

    public DataAnalysisService() {
        timer = new Timer();
    }

    public double max(String startTime, String endTime, long entitykey) {
        double ret = 0;
        logger.info("get max from start time = " + startTime + ", end time = " + endTime + ", entitykey = " + entitykey);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();         //BoolQueryBuilder query = QueryBuilders.boolQuery()            also ok
        boolQueryBuilder.filter(QueryBuilders.termQuery("entitykey", entitykey));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(startTime).lt(endTime));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(0);
        String agg_name = "agg_value";
        AggregationBuilder aggregation = AggregationBuilders.max(agg_name).field("value");
        searchSourceBuilder.aggregation(aggregation);

        searchSourceBuilder.query(boolQueryBuilder);

        Aggregations aggs = esHelper.GetAggregationBySearch(searchSourceBuilder, esserver_index);
        Max max = aggs.get(agg_name);
        ret = max.value();

        return ret;
    }

    public double min(String startTime, String endTime, long entitykey) {
        double ret = 0;
        logger.info("get min from start time = " + startTime + ", end time = " + endTime + ", entitykey = " + entitykey);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();         //BoolQueryBuilder query = QueryBuilders.boolQuery()            also ok
        boolQueryBuilder.filter(QueryBuilders.termQuery("entitykey", entitykey));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(startTime).lt(endTime));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(0);
        String agg_name = "agg_value";
        AggregationBuilder aggregation = AggregationBuilders.max(agg_name).field("value");
        searchSourceBuilder.aggregation(aggregation);

        searchSourceBuilder.query(boolQueryBuilder);

        Aggregations aggs = esHelper.GetAggregationBySearch(searchSourceBuilder, esserver_index);
        Min min = aggs.get(agg_name);
        ret = min.value();

        return ret;
    }

    public double average(String startTime, String endTime, long entitykey) {
        double ret = 0;
        logger.info("get average from start time = " + startTime + ", end time = " + endTime + ", entitykey = " + entitykey);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();         //BoolQueryBuilder query = QueryBuilders.boolQuery()            also ok
        boolQueryBuilder.filter(QueryBuilders.termQuery("entitykey", entitykey));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(startTime).lt(endTime));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(0);
        String agg_name = "agg_value";
        AggregationBuilder aggregation = AggregationBuilders.avg(agg_name).field("value");
        searchSourceBuilder.aggregation(aggregation);

        searchSourceBuilder.query(boolQueryBuilder);

        Aggregations aggs = esHelper.GetAggregationBySearch(searchSourceBuilder, esserver_index);
        Avg avg = aggs.get(agg_name);
        ret = avg.value();

        return ret;
    }

    public String stats(String startTime, String endTime, long entitykey) {
        String ret;
        logger.info("get stats from start time = " + startTime + ", end time = " + endTime + ", entitykey = " + entitykey);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();         //BoolQueryBuilder query = QueryBuilders.boolQuery()            also ok
        boolQueryBuilder.filter(QueryBuilders.termQuery("entitykey", entitykey));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(startTime).lt(endTime));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(0);
        String agg_name = "agg_value";
        AggregationBuilder aggregation = AggregationBuilders.stats(agg_name).field("value");
        searchSourceBuilder.aggregation(aggregation);

        searchSourceBuilder.query(boolQueryBuilder);

        Aggregations aggs = esHelper.GetAggregationBySearch(searchSourceBuilder, esserver_index);
        Stats stats = aggs.get(agg_name);
        ret = stats.toString();

        return ret;
    }

    /*public List<List<GridPower>> calTurbineLocation(String powerArray, int turbineNumber, int spaceXInMeters, int spaceYInMeters, int gridX, int gridY, int gridUnit) {
        List<List<GridPower>> ret = new ArrayList<List<GridPower>>();
        logger.info("calculate turbine location");

        String[] strPowerArray = powerArray.split(",");
        int[] iPowerArray = Arrays.stream(strPowerArray).mapToInt(Integer::parseInt).toArray();

        //Init grid power list
        List<GridPower> originalGridPowerList = new ArrayList<GridPower>();
        int k = 0;
        for(int i = 0; i < gridX; i++) {
            for(int j = 0; j < gridY; j++) {
                GridPower gridPower = new GridPower(i, j, iPowerArray[k++]);
                originalGridPowerList.add(gridPower);
                //gridPowerList.add() = iPowerArray[k++];
            }
        }
        //sort gridpower list by power
        List<GridPower> sortedGridPowerList = originalGridPowerList.stream().sorted(Comparator.comparing(GridPower::getPower).reversed()).collect(Collectors.toList());

        int maxSolution = 5;
        List<GridPower> resultList = new ArrayList<GridPower>();
        ret.add(resultList);
        //found best turbine location until reach turbineNumber
        int nextIndex = 0;
        for(int i = 0; i <= turbineNumber; i++) {

            boolean bExistOtherSolution = false;
            int previousPower = 0;
            for(int j = nextIndex; j < sortedGridPowerList.size(); j++){
                GridPower gridPower = sortedGridPowerList.get(j);

                if(gridPower.getPower() == previousPower) {     //found duplicate power, can create a new resultList

                }

                previousPower = gridPower.getPower();
                //check if meet space requirement
                if(checkSpaceReuqirement(gridPower, resultList, spaceXInMeters, spaceXInMeters, gridX, gridY, gridUnit)) {
                    resultList.add(gridPower);
                    gridPower.setSelected(true);        //will set sortedGridPowerList and originalGridPowerList?
                    nextIndex = j + 1;      //found the required grid, next time choose from next grid
                    break;                  //break inner loop
                }
            }
        }

        return ret;
    }*/

    public List<List<GridPower>> calTurbineLocation(String powerArray, int turbineNumber, int spaceXInMeters, int spaceYInMeters, int gridX, int gridY, int gridUnit) {
        List<List<GridPower>> ret = new ArrayList<List<GridPower>>();
        logger.info("calculate turbine location");

        String[] strPowerArray = powerArray.split(",");
        int[] iPowerArray = Arrays.stream(strPowerArray).mapToInt(Integer::parseInt).toArray();

        //Init grid power list
        List<GridPower> originalGridPowerList = new ArrayList<GridPower>();
        int k = 0;
        for(int i = 0; i < gridX; i++) {
            for(int j = 0; j < gridY; j++) {
                GridPower gridPower = new GridPower(i, j, iPowerArray[k++]);
                originalGridPowerList.add(gridPower);
                //gridPowerList.add() = iPowerArray[k++];
            }
        }
        //sort gridpower list by power
        List<GridPower> sortedGridPowerList = originalGridPowerList.stream().sorted(Comparator.comparing(GridPower::getPower).reversed()).collect(Collectors.toList());

        int maxSolution = 5;
        List<GridPower> firstResultList = new ArrayList<GridPower>();
        ret.add(firstResultList);
        //found best turbine location until reach turbineNumber
        int nextIndex = 0;
        for(int i = 0; i < turbineNumber; i++) {
            for(int sltnIndex = 0; sltnIndex < ret.size(); sltnIndex++) {
                List<GridPower> resultList = ret.get(sltnIndex);
                for(int j = nextIndex; j < sortedGridPowerList.size(); j++){
                    GridPower gridPower = sortedGridPowerList.get(j);
                    //check if meet space requirement
                    //if(checkSpaceReuqirement(gridPower, resultList, spaceXInMeters, spaceXInMeters, gridX, gridY, gridUnit)) {
                    Map<Integer, GridPower> zoneMap = getAllGridTooNear(gridPower, resultList, spaceXInMeters, spaceYInMeters, gridUnit);
                    if(zoneMap.size() == 0) {          //there are no zone too near
                        resultList.add(gridPower);
                        //gridPower.setSelected(true);        //will set sortedGridPowerList and originalGridPowerList?
                        nextIndex = j + 1;      //found the required grid, next time choose from next grid
                        break;                  //break inner loop
                    }
                    else if(zoneMap.size() == 1) {       //there are only 1 zone too near and equal to current power, create new resultlist
                        Map.Entry<Integer, GridPower> entry = zoneMap.entrySet().iterator().next();
                        int index = entry.getKey();
                        GridPower gridPowerOld = entry.getValue();
                        if(gridPowerOld.getPower() == gridPower.getPower()) {
                            if(ret.size() < maxSolution) {
                                List<GridPower> newResultList = new ArrayList<GridPower>(resultList);               //clone resultList
                                newResultList.set(index, gridPower);
                                ret.add(newResultList);
                            }
                        }
                    }
                }
            }
        }

        offsetList(ret, 1, 1);
        return ret;
    }

    //return false: not meet space requirement: there are wind turbine nearby
    private Map<Integer, GridPower> getAllGridTooNear(GridPower gridPower, List<GridPower> gridPowerList, int spaceXInMeters, int spaceYInMeters, int gridUnit) {

        Map<Integer, GridPower> ret = new HashMap<>();
        if(gridPowerList.size() > 0) {
            for(int i = 0; i < gridPowerList.size(); i++) {
                GridPower gridElement = gridPowerList.get(i);
                double xDiff = Math.abs(gridPower.getX() - gridElement.getX()) * gridUnit;
                double yDiff = Math.abs(gridPower.getY() - gridElement.getY()) * gridUnit;
                if(xDiff < spaceXInMeters && yDiff < spaceYInMeters) {
                    ret.put(i, gridElement);
                }
            }
        }

        return ret;
    }

    private void offsetList(List<List<GridPower>> list, int offsetX, int offsetY) {
        for(List<GridPower> listInternal : list) {
            for(GridPower item : listInternal) {
                item.offsetXY(offsetX, offsetY);
            }
        }
    }
    //return ture: meet space requirement: no wind turbine nearby
    //return false: not meet space requirement: there are wind turbine nearby
    /*private boolean checkSpaceReuqirement(GridPower gridPower, List<GridPower> originalGridPowerList, int spaceXInMeters, int spaceYInMeters, int gridX, int gridY, int gridUnit) {

        for(int i = 0; i < gridX; i++) {
            for (int j = 0; j < gridY; j++) {
                int index = i * gridX + j;
                GridPower gridElement = originalGridPowerList.get(index);
                if(gridElement.isSelected()) {
                    double xDiff = Math.abs(gridPower.getX() - gridElement.getX()) * gridUnit;
                    double yDiff = Math.abs(gridPower.getY() - gridElement.getY()) * gridUnit;
                    if(xDiff < spaceXInMeters && yDiff < spaceYInMeters) {
                        return false;
                    }
                }
            }
        }

        return true;
    }*/

    private double calDistanceBetweenGrid(GridPower gridPower1, GridPower gridPower2, int gridUnit) {

        double xDiff = gridPower1.getX() - gridPower2.getX();
        double yDiff = gridPower1.getY() - gridPower2.getY();
        double xyDiff = Math.sqrt(xDiff * xDiff + yDiff * yDiff);

        return xyDiff * gridUnit;
    }

    /*private List<TurbineModel> getMaxPowerGrid(int powerMatrix[][], int gridX, int gridY, List<TurbineModel> exceptTurbines) {

        int maxPower = 0;
        for(int i = 0; i < gridX; i++) {
            for(int j = 0; j < gridY; j++) {
                if(powerMatrix[i][j] > maxPower) {

                }

            }
        }
    }*/

    public double groupByEntity(String startTime, String endTime) {
        double ret = 0;
        logger.info("get max from start time = " + startTime + ", end time = " + endTime);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //boolQueryBuilder.filter(QueryBuilders.termQuery("entitykey", entitykey));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(startTime).lt(endTime));          //filter same with must but not count into score, it is fastest
        searchSourceBuilder.query(boolQueryBuilder);

        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(0);
        String agg_name = "agg_by_entitykey";
        TermsAggregationBuilder aggregation = AggregationBuilders.terms(agg_name).field("entitykey");
        AggregationBuilder subAggr = AggregationBuilders.stats("stats_value").field("value");
        aggregation.subAggregation(subAggr);
        searchSourceBuilder.aggregation(aggregation);

        Aggregations aggs = esHelper.GetAggregationBySearch(searchSourceBuilder, esserver_index);
        if(aggs != null) {
            Terms byEntityAggregation = aggs.get(agg_name);
            logger.info("aggregation by_entity result");
            logger.info("docCountError: " + byEntityAggregation.getDocCountError());
            logger.info("sumOfOtherDocCounts: " + byEntityAggregation.getSumOfOtherDocCounts());
            logger.info("------------------------------------");
            List<? extends Terms.Bucket> buckets = byEntityAggregation.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                Stats stats = bucket.getAggregations().get("stat_age");
                System.out.println(bucket.getKey()); //获取分组名称
                System.out.println("平均值：" + stats.getAvg());
                System.out.println("总数：" + stats.getSum());
                System.out.println("最大值：" + stats.getMaxAsString());
                System.out.println("最小值：" + stats.getMin());
            }
        }
        /*for(int i = 0; i < dataList.size(); i++) {
            Map<String, Object> data = dataList.get(i);
            long entitykey_find = Long.parseLong(data.get("entitykey").toString());
            if(entitykey_find == entitykey) {
                double value = Double.parseDouble(data.get("value").toString());
            }
        }*/
        //ret = dataList.stream().mapToDouble(p -> Double.parseDouble(p)).max().orElse(0);

        return ret;
    }

    public void startRollup() {
        //start rollupTask every minute
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, 1);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date nextMinute = calendar.getTime();
        System.out.println("Start rollup job from " + nextMinute.toString());

        rollTask = new RollupTask(esHelper, rollup_intervalminutes, rollup_rollupkeepdays, rollup_archivekeepdays, orginal_indices, rollup_index, archive_index, deleteAllOnStart);
        //timer.schedule (rollTask, 0, 1000 * 5);        //for testing purpose
        timer = new Timer();
        timer.schedule (rollTask, nextMinute, 1000*60);               //official release
    }

    public void stopRollup() {
        System.out.println("Stop rollup job");
        timer.cancel();
    }

    private boolean checkIndexExist(String... indices) {
        return esHelper.IndexExist(indices);
    }
}

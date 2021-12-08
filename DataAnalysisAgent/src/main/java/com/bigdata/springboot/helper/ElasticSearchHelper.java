package com.bigdata.springboot.helper;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
//import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

@Component
public class ElasticSearchHelper {

    private RestHighLevelClient client;

    public ElasticSearchHelper() {}

    public ElasticSearchHelper(String host, int port1, int port2) {
        Init(host, port1, port2);
    }

    public void Init(String host, int port1, int port2) {
        System.out.println("host=" + host + ", port1=" + port1 + ", port2=" + port2);
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port1, "http"),
                        new HttpHost(host, port2, "http")));
    }

    public void Close() {
        try {
            client.close();
            System.out.println("Close Elasticsearch client");
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
    }
	
    public boolean ping() {
        boolean ret = false;
        try {
            ret = client.ping(RequestOptions.DEFAULT);
        } catch (IOException e) { }
        return ret;
    }

    public boolean CreateIndex(String index) {
        return CreateIndex(index, "text");
    }

    //This function is not complete yet, do not use it
    public boolean CreateIndex(String index, String type) {
        CreateIndexRequest request = new CreateIndexRequest(index);
        boolean acknowledged = false;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    builder.startObject("message");
                    {
                        builder.field("type", type);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping(builder);

            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            acknowledged = createIndexResponse.isAcknowledged();
            System.out.println("Create index " + index + " result: " + acknowledged);
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }

        return acknowledged;
    }

    public boolean DeleteIndex(String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        boolean acknowledged = false;
        try {
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            acknowledged = deleteIndexResponse.isAcknowledged();
            System.out.println("delete index " + index + " result: " + acknowledged);
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    public boolean IndexExist(String... indices) {
        GetIndexRequest request = new GetIndexRequest(indices);

        boolean exists = false;
        try {
            exists = client.indices().exists(request, RequestOptions.DEFAULT);
            System.out.println("Exist index " + String.join(",", indices) + ", result: " + exists);
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return exists;
    }

    public boolean OpenIndex(String index) {
        OpenIndexRequest request = new OpenIndexRequest(index);
        boolean acknowledged = false;
        try {
            OpenIndexResponse openIndexResponse = client.indices().open(request, RequestOptions.DEFAULT);
            acknowledged = openIndexResponse.isAcknowledged();
            System.out.println("open index " + index + " result: " + acknowledged);
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    public boolean CloseIndex(String index) {
        CloseIndexRequest request = new CloseIndexRequest(index);
        boolean acknowledged = false;
        try {
            AcknowledgedResponse closeIndexResponse = client.indices().close(request, RequestOptions.DEFAULT);
            acknowledged = closeIndexResponse.isAcknowledged();
            System.out.println("close index " + index + " result: " + acknowledged);
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }

        return acknowledged;
    }

    //insert single data
    public boolean InsertTextData(String index, String content) {
        return InsertTextData(index, null, content);
    }

    public boolean InsertTextData(String index, String id, String content) {
        IndexRequest indexRequest;
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("message", content);
        if (id == null) {
            //id = UUID.randomUUID().toString();
            //indexRequest = new IndexRequest(index).type("_doc").source(dataMap);              //deprecated
            indexRequest = new IndexRequest(index).source(dataMap);
        } else {
            //indexRequest = new IndexRequest(index).type("_doc").id(id).source(dataMap);       //deprecated
            indexRequest = new IndexRequest(index).id(id).source(dataMap);
        }
        boolean acknowledged = false;
        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                acknowledged = true;
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                acknowledged = true;
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //insert Jason data
    public boolean InsertMapData(String index, Map<String, Object> dataMap) {
        //IndexRequest indexRequest = new IndexRequest(index).type("_doc").source(dataMap);           //deprecated
        IndexRequest indexRequest = new IndexRequest(index).source(dataMap);
        boolean acknowledged = false;
        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                acknowledged = true;
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                acknowledged = true;
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //insert Jason data
    public boolean InsertOrUpdateMapDataWithId(String index, String id, Map<String, Object> dataMap) {
        IndexRequest indexRequest = new IndexRequest(index).type("_doc").id(id).source(dataMap);
        boolean acknowledged = false;
        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                acknowledged = true;
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                acknowledged = true;
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //delete data by index and id
    public boolean DeleteData(String index, String id) {
        DeleteRequest request = new DeleteRequest(index).id(id);
        boolean acknowledged = false;
        try {
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                acknowledged = true;
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //get data from index by id
    public String GetData(String index, String id) {
        GetRequest getRequest = new GetRequest(index, id);
        //getRequest.type("_doc");
        String sourceAsString = null;
        try {
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
                sourceAsString = response.getSourceAsString();
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return sourceAsString;
    }

    //get data from index by sourceBuilder
    public SearchResponse GetResponseBySearch(SearchSourceBuilder sourceBuilder, String... indics) {
        //SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest(indics);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return searchResponse;
    }

    //get data from index by sourceBuilder
    public List<Map<String, Object>> GetDataBySearch(SearchSourceBuilder sourceBuilder, String... indics) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        SearchResponse searchResponse = GetResponseBySearch(sourceBuilder, indics);
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();
        if (RestStatus.OK.equals(searchResponse.status())) {
            SearchHits hits = searchResponse.getHits();
            long totalHits = hits.getTotalHits().value;
            if (totalHits > 0) {
                SearchHit[] searchHits = hits.getHits();
                for (SearchHit hit : searchHits) {
                    // do something with the SearchHit
                    String index1 = hit.getIndex();
                    String id = hit.getId();
                    float score = hit.getScore();
                    String sourceAsString = hit.getSourceAsString();
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    //results.add(sourceAsString);
                    results.add(sourceAsMap);
                }
            }
        }

        return results;
    }

    //get data from index by sourceBuilder
    //public List<Map<String, Object>> GetAggregationBySearch(String index, SearchSourceBuilder sourceBuilder, String agg_name) {
    public Aggregations GetAggregationBySearch(SearchSourceBuilder sourceBuilder, String... indics) {
        //List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        Aggregations ret = null;
        SearchResponse searchResponse = GetResponseBySearch(sourceBuilder, indics);
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();
        if (RestStatus.OK.equals(searchResponse.status())) {
            ret = searchResponse.getAggregations();
            /*Map<String, Aggregation> aggrAsMap = aggregations.asMap();
            if (aggrAsMap.size() > 0) {
                for (Map.Entry<String, Aggregation> entry : aggrAsMap.entrySet()) {
                    String key = entry.getKey();
                    Aggregation aggregation = entry.getValue();
                    String temp = aggregation.toString();
                    System.out.println(aggregation);
                    Map<String, Object> temp1 = aggregation.getMetadata();
                    System.out.println(temp1);
                }
            }*/
        }

        return ret;
    }

    //get data by MultiSearchRequest, this function is not finish yet
    public List<String> GetDataByMultiSearch(MultiSearchRequest mSequestRequest) {
        List<String> results = new ArrayList<String>();
        try {
            MultiSearchResponse mSearchResponse = client.msearch(mSequestRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = mSearchResponse.getResponses()[0].getResponse();
            RestStatus status = searchResponse.status();
            TimeValue took = searchResponse.getTook();
            Boolean terminatedEarly = searchResponse.isTerminatedEarly();
            boolean timedOut = searchResponse.isTimedOut();
            if (timedOut == false) {
                SearchHits hits = searchResponse.getHits();
                long totalHits = hits.getTotalHits().value;
                if (totalHits > 0) {
                    SearchHit[] searchHits = hits.getHits();
                    for (SearchHit hit : searchHits) {
                        // do something with the SearchHit
                        String index1 = hit.getIndex();
                        String id = hit.getId();
                        float score = hit.getScore();
                        String sourceAsString = hit.getSourceAsString();
                        results.add(sourceAsString);
                    }
                }
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return results;
    }

    //bulky insert Jason data; autoId: true will use ES uuid, false will increase from 1, it will override existint doc with same id
    public boolean BulkyInsertData(String index, List<Map<String, Object>> dataMapList, boolean autoId) {
        if (dataMapList == null || dataMapList.size() == 0)
            return false;

        BulkRequest request = new BulkRequest();
        for (int i = 0; i < dataMapList.size(); i++) {
            Map<String, Object> dataMap = dataMapList.get(i);
            if(autoId) {
                IndexRequest indexRequest = new IndexRequest(index).source(dataMap);
                request.add(indexRequest);
            }
            else {
                IndexRequest indexRequest = new IndexRequest(index).id(String.valueOf(i + 1)).source(dataMap);
                request.add(indexRequest);
            }
        }

        boolean acknowledged = false;
        try {
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures() == true) {
                acknowledged = false;
                System.out.println(response.buildFailureMessage());
            } else {
                acknowledged = true;
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return acknowledged;
    }

    //delete data by index and query
    public long DeleteDataByQuery(QueryBuilder query, String... indices) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(indices);
        request.setConflicts("proceed");
        request.setQuery(query);
        request.setTimeout(TimeValue.timeValueMinutes(2));
        request.setRefresh(true);
        //boolean acknowledged = false;
        long ret = -1;
        try {
            BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
            BulkByScrollTask.Status status = response.getStatus();
            boolean timedOut = response.isTimedOut();
            ret = response.getDeleted();
            if (timedOut) {
                System.out.println("timeOut when execute DeleteByQueryRequest");
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return ret;
    }

    //delete data by index and query
    public long DeleteAllDataInInIndices(String... indices) {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        return DeleteDataByQuery(query, indices);
    }

    //reindex data from sources to dest
    public long reIndex(QueryBuilder queryBuilder, String destIndex, String... sourceIndices) {
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices(sourceIndices);
        request.setDestIndex(destIndex);
        request.setDestVersionType(VersionType.EXTERNAL);
        request.setDestOpType("create");
        request.setConflicts("proceed");
        request.setTimeout(TimeValue.timeValueMinutes(2));
        request.setRefresh(true);
        if(queryBuilder != null)
            request.setSourceQuery(queryBuilder);

        long ret = 0;
        try {
            BulkByScrollResponse response = client.reindex(request, RequestOptions.DEFAULT);
            ret = response.getTotal();
            int failCount = response.getBulkFailures().size();
            if(failCount > 0) {
                System.out.println("ReIndex failure count: " + failCount);
            }
            else {
                System.out.println("ReIndex execute successfully");
            }
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
        } catch (java.io.IOException ex) {
            System.out.println(ex.getLocalizedMessage());
        }
        return ret;
    }
}
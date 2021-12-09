# Rquirement
1. Collect Wind farm data from PMSAgnet, the data is time series based. Design the time series data structure for them, save to time series database and processing, provide statistic API for FrontEnd.
2. Wind farm location design function: given an area and wind farm types, **modelling** and design your own **algorithm** to choose best location for these wind farms.
![](./docs/images/windfarm.jpg)
# Analysis & Design
1. Time Series Data Collect function: 
    + Read the wind farm real-time data from PMSAgent and save to **Time Series Database**. 
    + Provide interface to FrontEnd **BI** for statistic. 
    + We use **OpenTsDB** in before, now change to **ElasticSearch**, because ES is better than opentsdb in terms of statistics. 
    + Data structure is datapointname + value + timestamp. 
    + Function inlclude statistic by hour, day, month and year
2. Rollup function:  
If each wind farm create an index in ElasticSearch, take 30 wind farm for example, each wind farm 30 datapoint, collect data each second, there will be 30*200*3600*24=518400000 data, around 500 million records per day. So much design rollup function.
3. Wind farm location design function:  
    + There are 2 types of wind farm: 2MW and 3MW, their minimum spacing in X and Y is different
    + Use 50 * 50 meters grid in model, each grid contain only 1 wind fram
    + There are totally 198 * 134 = 26532 grid
    + If all choose 2MW wind farm, there will be 25 wind farms in total; if all choose 3MW, there will be 16 wind farms in total. mix selection not considered now.
    + Provide the maximum power generation capacity under the condition that the adjacent wind farm are not less than the minimum distance
4. Develop framework choose: 
    + BackEnd DataAnalysisAgent use Java and Springboot
    + FrontEnd I only provide a simple demo written by Angular framework, real job will be finished by FrontEnd team.

# BackEnd Implementation
Split job to 3 part :: ES CRUD, Rollup and wind farm location selection algorithm
## Rollup
Elastic search have own rollup function, but I foundit is only a trial version, also it cannot delete existing data after I test. So it does not meet our requirements, I must design the roll-up function by myself.  
  
There are 7 parameters in my rollup fuction:
Parameter Name | Value | Description
---- | ---- | ----
rollup.orginalindices | windspeed1,windspeed2	|	original indics of all wind turbine
rollup.rollupindex | windspeed_rollup |			index after rollup
rollup.archiveindex | windspeed_archive |			archive after old data
rollup.intervalminutes | 15 |				rollup interval in minutes
rollup.rollupkeepdays | 7 |					data kept in rollup index
rollup.archivekeepdays | 7 |				data kept in archive index
rollup.deleteallonstart | false |				if delete all unrollup data when agent start

archiveindex and orginalindex have same structure，value can be max_value, min_value, avg_value by rollupindex

### Rollup Task
RollupTask.java do all the rollup job, since original data is too big, so rollup job must be done every minute exactly, below code can make sure execute by each minute punctually:
```` Java
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, 1);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date nextMinute = calendar.getTime();

        timer = new Timer();
        timer.schedule (rollTask, nextMinute, 1000*60);
````
RollupTask has the following steps:  
1) roll up all data from startTime to endTime by intervalMinutes
2) reindex all data from original indices to achiveindex
3) delete old data in original indices to optimize rollup speed

## ES CRUD
### Create Index
Create Index by PostMan:  
Post to http://10.4.62.59:9200/windspeed1
````json
{
	"mappings":{
		"properties":{
			"pkey":{
				"type":"keyword"
			},
			"locationkey":{
				"type":"long"
			},
			"devicekey":{
				"type":"long"
			},
			"entitykey":{
				"type":"long"
			},
			"typename":{
				"type":"long"
			},
			"value":{
				"type":"long"
			},
			"timestamp":{
				"type":"date",
				"format":"yyyy-MM-dd HH-mm-ss"
			}
		}
	}
}
````
Note that when using RangeQuery for timestamp, the lt, gt, lte, gte, to, from function parameters are strings, and the format must be the same as the format of the defined index.
````java
        String endTime = archiveDeadline.format(formatter) + "00:00:00";
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("timestamp").lt(endTime);
````
If the endTime here only has yyyy-MM-dd but not HH-mm-ss, the query will give strange results, traps
RangeQuery, using lt, gt is more explicit than to, from, it is recommended to use lt, gt, etc.

### ES query, 
where condition uses TermQuery, multiple condition query, use BoolQuery to connect TermQuery, if you need to group the results, use AggregationQuery, the above query is written in exactly the same way.
````java
        SearchRequest searchRequest = new SearchRequest(indics);
        searchRequest.source(sourceBuilder); //SearchSourceBuilder sourceBuilder, including QueryBuilder
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
````
The difference between query and delete, update and reindex is that it uses SourceBuilder as an input parameter. The other several are directly used QueryBuilder. SourceBuilder includes QueryBuilder. Because the query is more complex, aggregation AggregationBuilder can be used, and SortBuilder can be sorted.

1) Aggregation queries can be nested, generally use terms for first-level aggregation, such as
````java
TermsAggregationBuilder aggregation = AggregationBuilders.terms("agg_by_entitykey").field("entitykey");
The secondary query is generally used to find the maximum, minimum, average, or all, such as
AggregationBuilder subAggr = AggregationBuilders.stats("stats_value").field("value");
aggregation.subAggregation(subAggr);
//Then merge into sourceBuilder
searchSourceBuilder.aggregation(aggregation);

//The result of the aggregation query is to return a bucket,
Terms byEntityAggregation = aggs.get(agg_name);
        List<? extends Terms.Bucket> buckets = byEntityAggregation.getBuckets();
````
I feel that it’s not too late to look again when you need to use aggregate queries in the future, and you don’t have to remember the details.

2) Bool query corresponds to BooleanQuery in Lucene, which consists of one or more clauses, each clause has a specific type, this is a compound query

+ .must  
The returned documents must meet the conditions of the must clause and participate in the calculation of scores

+ .filter  
The returned document must meet the conditions of the filter clause, but it will not participate in the calculation of scores like must

+ .should  
The returned document may meet the conditions of the should clause. In a bool query, if there is no must or filter, and there are one or more should clauses, then as long as one meets one, it can be returned. The minimum_should_match parameter defines at least several clauses to be satisfied.

+ .mustNot  
The returned document must not meet the defined conditions  

Try to use filter, it is faster in two ways:
1) Cache the results
2) Avoid calculating points

example:
````java
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder(); 
        //BoolQueryBuilder query = QueryBuilders.boolQuery() also ok
        boolQueryBuilder.filter(QueryBuilders.termQuery("entitykey", entitykey));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(startTime).lt(endTime));
````

### ES Delete
The deletion of ES is actually similar to query
````java
        DeleteByQueryRequest request = new DeleteByQueryRequest(indices);
        request.setConflicts("proceed");
        request.setQuery(query); //QueryBuilder query
        request.setTimeout(TimeValue.timeValueMinutes(2));
        request.setRefresh(true);
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
````

### ES Reindex
ES reindex is to copy the data of one index to another (the original one will not be deleted), and the writing method is similar to query and delete
````java
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices(sourceIndices);
        request.setDestIndex(destIndex);
        request.setDestVersionType(VersionType.EXTERNAL);
        request.setDestOpType("create");
        request.setConflicts("proceed");
        request.setTimeout(TimeValue.timeValueMinutes(2));
        request.setRefresh(true);
        if(queryBuilder != null)
            request.setSourceQuery(queryBuilder); //Can also bring query

        BulkByScrollResponse response = client.reindex(request, RequestOptions.DEFAULT);
````
### ES Update
ES update is also similar, but time series data is not needed, so this project is not implemented.
### ES CRUD Summarize
All CRUD operations of ES can use String... indics to operate multiple indexes. Except for Insert, the operation of Insert is called IndexRequest, and BulkRequest can combine IndexRequest.  
This is very strange. In addition, there are other operations on Index. Note that they have no inheritance relationship with IndexRequest:
+ CreateIndexRequest
+ DeleteIndexRequest
+ GetIndexRequest //check if index exist
+ OpenIndexRequest
+ CloseIndexRequest

For various requests, you only need to know that ES can complete these functions, and come back when you use them in the future.

## Wind farm location selection algorithm
The algorithm for wind power location selection is very complicated, and what you can achieve by yourself is not necessarily the best one. Just figure out the standards.
Input parameters:
1) The power generation and size of each grid (50m), the number of grids in X and Y directions (198*134)
2) Total number of fans
3) The minimum distance between adjacent fans in the X and Y directions
Require:
Provide the maximum power generation capacity under the condition that the adjacent wind farm are not less than the minimum distance

calTurbineLocation() function in DataAnalysisService.java implement my algorithm, below is the algorithm description:

### My Algorithm
1) Sort the grid list by power:  
````java
List<GridPower> sortedGridPowerList = originalGridPowerList.stream().sorted(Comparator.comparing(GridPower::getPower).reversed()).collect(Collectors.toList());
````
The stream().sorted() method is used here. Comparator.comparing is sorted from small to large, so .reversed() is called again. The writing method is complicated, it is recommended to copy in the future

2) Select the grid with the largest power.  
3) Then see if the grid with the second largest power generation meets the distance requirement ( only need (x1-x2)*unit <x_min and (y1-y2)*unit < y_min, because the x minimum spacing and y minimum spacing are independent ). If satisfied, selected; if not, skip it.
4) Then use same rule to check next grid in the sorted list, if it meets the distance requirements from the previous selected grid, selected; if not, skip it.  
5) Repeat previous step until the total number of wind farms required.

# FrontEnd Demo
I need provide a demo to FrontEnd team, they will work on it fianlly. I choose the popular **Angular** framework. It use typescript as main develop language. To start Angular project, use command:
+ $ ng server  

Below is main file description:
## app.gridpower.ts  
Define class GridPower and array GridPowers for selected grid.
## app.locationmodel.ts  
Define whole map configuration parameter
## app.component.ts
It is main typescript in this demo, bind app.component.html and app.component.css together, embed this html into app-root in index.html.
````typescript
@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
````

## app.component.html
Standard html writing include angular grammar:
1. [(ngModel)]="gridNumberX"  or {{name}}  
This statement will bind the input value with gridNumberX variable in app.component.ts, it is two-way bind.
2. (click)="onClickPost('2MW')"
This statement will bind button click event with onClickPost() function in app.component.ts, and pass 2MW as parameter.

The following are the variables exported by the .ts file, which can be quoted in html with {{}}. Each line is an exported variable. The exported class will export all the variables of this class:  
````typescript
export class AppComponent {
   title ='my-first-angular-app hongzhi';
   name: any;

   postData = {
     test:'mycontent',
   };

   url ='http://httpbin.org/post';
   json;
````

The typescript variable type is suffixed, and the variable type can also be omitted in the case of initialization, just like golang
````typescript
   private powerValues: number[]; //Array
   private girdPowers: GridPower[];
   private solutions: GridPower[][]; //Two-dimensional array

   postData = {//Map type
     test:'mycontent',
   };
````

AfterViewInit is called after the page template is initialized, and the value is called once
export class AppComponent implements AfterViewInit {
````typescript
  @ViewChild('canvas1') canvas1: ElementRef; //Mapping html page elements and variables, here is a reference
  @ViewChild('select1') solutionDropList: ElementRef;
  @ViewChild('lblDetail') lblDetail: ElementRef;
  private context: CanvasRenderingContext2D;
````
The following is how to write AfterViewInit inside Angular
````typescript
interface AfterViewInit {
  ngAfterViewInit(): void
}
````
ngAfterViewInit() must be realized

````typescript
private context: CanvasRenderingContext2D;
ngAfterViewInit(): void {
    this.context = (this.canvas1.nativeElement as HTMLCanvasElement).getContext('2d');
    this.drawGrid();
  }
````
After getting the context, you can use it to draw various graphics, drawGrid(), drawCircle(), drawText()

The post function is implemented with HttpClient and passed in the constructor. The constructor is injection. The constructor is a new concept of ES6. I think there is only one purpose for injection.
````typescript
constructor(private httpClient: HttpClient) {
  }

this.httpClient.post(this.url, locationModel).toPromise().then((data:any) => {
}
````
The input parameter 1 of the post function is url, and 2 is any format parameter, which will be converted into a json string. The return value data is determined by the backend return value. DataAnalysisAgent returns a json string:
````java
List<List<GridPower>> value = dataAnalysisService.calTurbineLocation(powerArray, turbineNumber, spaceXInMeters, spaceYInMeters, gridX, gridY, gridUnit);
Map<String, String> data = new HashMap<String, String>() {{ put("value", gson.toJson(value)); }};
````
Here this.solutions = JSON.parse(data.value); parsed into GridPower[][] two-dimensional array, and then redraw the entire page

onChangeSolution() is a solution to switch between 2MW and 3MW

## License

This application is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details..
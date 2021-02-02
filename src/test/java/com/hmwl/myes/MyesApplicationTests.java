package com.hmwl.myes;

import com.google.gson.Gson;
import entity.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class MyesApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @Test
    void contextLoads() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("user2");
        CreateIndexResponse indexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
    }
    /**
     * 测试索引是否存在
     */
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("user2");
        boolean exist = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("索引是否存在："+exist);
    }

    /**
     * 测试indexAPi
     */
    @Test
    public void testIndexApi(){
        Map<String, Object> param = new HashMap<>();
        param.put("name","martin");
        param.put("code","11111111");
        param.put("job","programer");

    }

    /**
     * 删除index
     */
    @Test
    public void deleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("user2");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
    /********************* 文档操作 ***************************/
    @Test
    public void createNewDocuments() throws IOException {
        User user = new User("1111","martin","25");
        IndexRequest indexRequest = new IndexRequest("user1");
        indexRequest.id("1");
        indexRequest.timeout("1s");
        Gson gson = new Gson();
        indexRequest.source(gson.toJson(user),XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("创建文档结果:"+index.status());
    }
    /**
     * 判断文档是否存在
     */
    @Test
    public void testExistDocument() throws IOException {
        GetRequest request = new GetRequest("user1", "2");
        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        System.out.println("文档是否存在:"+exists);
    }
    /**
     * 获取文档
     */
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("user1","1");
        GetResponse doc = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(doc.getSourceAsString());
    }
    /***
     * 修改文档
     */
    @Test
    public void updateDocument() throws IOException {
        User user = new User();
        user.setAge("18");
        UpdateRequest request = new UpdateRequest("user1","1");
        request.timeout(TimeValue.MINUS_ONE);
        Gson gson = new Gson();
        request.doc(gson.toJson(user),XContentType.JSON);
        UpdateResponse result = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(result);
    }
    /**
     * 测试删除文档
     */
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("user1","1");
        request.timeout("1s");
        DeleteResponse result = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(result);
    }

    /**
     * 批量插入文档
     */
    @Test
    public void testBatchAddDocument() throws IOException {
        List<User> userList = new ArrayList<>();
        userList.add(new User("222","张三","11"));
        userList.add(new User("223","张四","11"));
        userList.add(new User("224","张五","11"));
        userList.add(new User("225","张刘","11"));
        userList.add(new User("226","张七","11"));
        userList.add(new User("227","张八","11"));
        Gson gson = new Gson();
        BulkRequest request = new BulkRequest();
        request.timeout("1s");
        for(int i =0;i<userList.size();i++){
            request.add(new IndexRequest("user1").
                    id(""+(i+1)).
                    source(gson.toJson(userList.get(i)),XContentType.JSON));
            BulkResponse result = restHighLevelClient.bulk(request,RequestOptions.DEFAULT);
            System.out.println(result.status());
        }

    }
    /**
     * 查询文档
     */
    @Test
    public void searchDocument() throws IOException {
        SearchRequest request = new SearchRequest("user1");
        // builder condition 构建查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 高亮
        sourceBuilder.highlighter();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "张四");
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(6000));
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit:
             response.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }

}

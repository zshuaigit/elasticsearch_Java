package com.zshuai.elastic.springboot_elastic.controller;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONObject;
import com.zshuai.elastic.springboot_elastic.entity.DemoEntity;
import com.zshuai.elastic.springboot_elastic.entity.PagingVO;
import com.zshuai.elastic.springboot_elastic.utils.ChineseName;
import com.zshuai.elastic.springboot_elastic.utils.ElasticSearchUtil;
import com.zshuai.elastic.springboot_elastic.utils.JsonUtils;

@RestController
@RequestMapping("/demo")
public class EsDemoController {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(EsDemoController.class);

	private String indexName = "index_test";
	
	private String typeName = "type_test";
	
	/**  
	 * <p>Title: createIndex</p>  
	 * <p>Description: 创建索引 </p>  
	 * @return  
	 */
	@RequestMapping("/createIndex")
	public String createIndex(HttpServletRequest request, HttpServletResponse response) {
		LOGGER.info("创建索引测试！");
		//先测试索引是否存在
		if (!ElasticSearchUtil.isIndexExist(indexName)) {
			ElasticSearchUtil.createIndex(indexName);
		}else {
			return "索引已经存在！";
		}
		return "索引创建成功！";
	}
	
	

    /**  
     * <p>Title: insertPojo</p>  
     * <p>Description: 插入数据 </p>  
     * @return  
     */  
    @RequestMapping("/insert")
    public String insertPojo() {
    	DemoEntity pojo = new DemoEntity();
    	pojo.setId(UUID.randomUUID().toString().replaceAll("-", ""));
    	pojo.setAge(new Random().nextInt(100));
    	pojo.setName((String) ChineseName.getAddress().get("name"));
    	//pojo.setName("宗尔");
    	pojo.setTimestamp(new Date().getTime()+"");
        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(pojo);
        String id = ElasticSearchUtil.addData(jsonObject, indexName, typeName, jsonObject.getString("id"));
        return id;
    }
    
    /**  
     * <p>Title: delete</p>  
     * <p>Description: 删除数据根据ID </p>  
     * @param id
     * @return 
     * http://localhost:8010/demo/delete?id=c7652a20d2794550a62b24b3ba02fbcb 
     */  
    @RequestMapping("/delete")
    public String delete(String id) {
        if (StringUtils.isNotBlank(id)) {
        	ElasticSearchUtil.deleteDataById(indexName, typeName, id);
            return "删除id=" + id;
        } else {
            return "id为空";
        }
    }
    
    /**  
     * <p>Title: update</p>  
     * <p>Description: 更新数据 </p>  
     * @param id
     * @return  
     */  
    @RequestMapping("/update")
    public String update(String id) {
        if (StringUtils.isNotBlank(id)) {
        	DemoEntity pojo = new DemoEntity();
        	pojo.setId(id);
        	pojo.setAge(new Random().nextInt(100));
        	pojo.setName("宗师");
        	pojo.setTimestamp(new Date().getTime()+"");
            JSONObject jsonObject = (JSONObject) JSONObject.toJSON(pojo);
            ElasticSearchUtil.updateDataById(jsonObject, indexName, typeName, id);
            return "修改成功！id=" + id;
           
        } else {
            return "id为空";
        }
    }
    
    /** 
     * http://localhost:8010/demo/getData?id=bd2d1ed86f1140468a67a64dea77590d 
     * <p>Title: getData</p>  
     * <p>Description: </p>  
     * @param id
     * @return  
     */  
    @RequestMapping("/getData")
    public String getData(String id) {
        if (StringUtils.isNotBlank(id)) {
        	//Map<String, Object> map = ElasticSearchUtil.searchDataById(indexName, typeName, id, "age,id");//返回值只显示age和ID
            Map<String, Object> map = ElasticSearchUtil.searchDataById(indexName, typeName, id, null);
            //return JSONObject.toJSONString(map);
            String string = map.toString();
            String objectToJson = JsonUtils.objectToJson(map);
            return objectToJson;
        } else {
            return "id为空";
        }
    }
    
    /**
     * 查询数据
     * 模糊查询
     *
     * @return
     */
    /**  
     * <p>Title: queryMatchData</p>  
     * <p>Description: 查询数据   模糊查询</p>  
     * @return  
     */  
    @RequestMapping("/queryMatchData")
    public String queryMatchData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolean matchPhrase = false;
        if (matchPhrase == Boolean.TRUE) {
            //不进行分词搜索
            boolQuery.must(QueryBuilders.matchPhraseQuery("name", "宗帅"));
        } else {
            boolQuery.must(QueryBuilders.matchQuery("name", "宗帅"));
        }
        List<Map<String, Object>> list = ElasticSearchUtil. searchListData(indexName, typeName, boolQuery, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }

    /**  
     * <p>Title: queryWildcardData</p>  
     * <p>Description:通配符查询数据 通配符查询 ?用来匹配1个任意字符，*用来匹配零个或者多个字符 </p>  
     * @return  
     */  
    @RequestMapping("/queryWithName")
    public String queryWithName() {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name.keyword", "宗?");//"宗*"
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, typeName, queryBuilder, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }
    
    
    /**  
     * <p>Title: queryRegexpData</p>  
     * <p>Description:正则查询 [0-9]{1,13} 匹配0-9之间的数字，1到13次</p>  
     * @return  
     */  
    @RequestMapping("/queryRegexpData")
    public String queryRegexpData() {
        QueryBuilder queryBuilder = QueryBuilders.regexpQuery("timestamp.keyword", "[0-9]{1,13}");
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, typeName, queryBuilder, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }
    
    /**  
     * <p>Title: queryIntRangeData</p>  
     * <p>Description: 查询指定字段数值的数字范围数据</p>  
     * @return  
     */  
    @RequestMapping("/queryIntRangeData")
    public String queryIntRangeData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("age").from(21)  .to(25));
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, typeName, boolQuery, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }
    
    /**  
     * <p>Title: queryDateRangeData</p>  
     * <p>Description: 查询日期范围数据</p>  
     * @return  
     */  
    @RequestMapping("/queryDateRangeData")
    public String queryDateRangeData() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery("timestamp").from("1566793498609")
                .to("1566795314479"));
        List<Map<String, Object>> list = ElasticSearchUtil.searchListData(indexName, typeName, boolQuery, 10, null, null, null);
        return JSONObject.toJSONString(list);
    }

    
    /** 
     * http://localhost:8010/demo/queryPage?startPage=3&pageSize=2 
     * <p>Title: queryPage</p>  
     * <p>Description: 分页查询 </p>  
     * @param startPage 页码
     * @param pageSize  每页显示的条数
     * @return  
     */  
    @RequestMapping("/queryPage")
    public String queryPage(String startPage, String pageSize) {
        if (StringUtils.isNotBlank(startPage) && StringUtils.isNotBlank(pageSize)) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            //boolQuery.must(QueryBuilders.rangeQuery("timestamp").from("1566793498609") .to("1566795314479"));
            PagingVO list = ElasticSearchUtil.searchDataPage(indexName, typeName, Integer.parseInt(startPage), Integer.parseInt(pageSize), boolQuery, null, null, null);
            
            List<Map<String, Object>> recordList = list.getRecordList();
            return JSONObject.toJSONString(list);
        } else {
            return "参数缺失！";
        }
    }



}

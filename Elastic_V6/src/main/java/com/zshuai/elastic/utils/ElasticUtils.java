package com.zshuai.elastic.utils;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSONObject;
import com.zshuai.elastic.config.ElasticConfig;
import com.zshuai.elastic.entity.Article;


public class ElasticUtils {

	/**
	 * Title: creatIndex Description: 创建索引
	 * 
	 * @param       name：elastic名字
	 * @param       host：主机地址
	 * @param index ：创建的索引名字
	 */
	public static boolean createIndex(String name, String host, String index) {
		// 获取客户端
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(name, host);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (!isIndexExist(name, host, index)) {
			System.out.println("Index is not exits!");
		}
		CreateIndexResponse indexresponse = client.admin().indices().prepareCreate(index).execute().actionGet();
		System.out.println("执行建立成功？" + indexresponse.isAcknowledged());
		client.close();
		return indexresponse.isAcknowledged();
	}

	/**
	 * Title: deleteIndex Description: 删除索引
	 * 
	 * @param name
	 * @param host
	 * @param index
	 * @return
	 */
	public static boolean deleteIndex(String name, String host, String index) {
		if (!isIndexExist(name, host, index)) {
			System.out.println("Index is not exits!");
		}
		// 获取客户端
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(name, host);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
		if (dResponse.isAcknowledged()) {
			System.out.println("delete index " + index + "  successfully!");
		} else {
			System.out.println("Fail to delete index " + index);
		}
		client.close();
		return dResponse.isAcknowledged();
	}

	/**
	 * Title: isIndexExist Description:判断要创建的索引是否存在
	 * 
	 * @param name
	 * @param host
	 * @param index
	 * @return
	 */
	public static boolean isIndexExist(String name, String host, String index) {
		// 获取客户端
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(name, host);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index))
				.actionGet();
		if (inExistsResponse.isExists()) {
			System.out.println("Index [" + index + "] is exist!");
		} else {
			System.out.println("Index [" + index + "] is not exist!");
		}
		client.close();
		return inExistsResponse.isExists();
	}

	/**
	 * Title: isTypeExist Description: 判断索引下的type是否存在
	 * 
	 * @param name
	 * @param host
	 * @param index
	 * @param type
	 * @return
	 */
	public boolean isTypeExist(String name, String host, String index, String type) {
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(name, host);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isIndexExist(name, host, index)
				? client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists()
				: false;
	}

	/**
	 * Title: addData Description:添加数据
	 * 
	 * @param jsonObject
	 * @param name
	 * @param host
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	public static String addData(JSONObject jsonObject, String name, String host, String index, String type,
			String id) {
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(name, host);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		IndexResponse response = client.prepareIndex(index, type, id).setSource(jsonObject).get();
		System.out.println("addData response status:{},id:{}" + response.status().getStatus() + "," + response.getId());
		client.close();
		return response.getId();
	}

	/**
	 * 插入记录
	 *
	 * @return
	 */
	public static String insertPojo(Article pojo) {
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		JSONObject jsonObject = (JSONObject) JSONObject.toJSON(pojo);
		IndexResponse response = client.prepareIndex(ElasticConfig.INDEX, ElasticConfig.TYPE, pojo.getId())
				.setSource(jsonObject).get();
		System.out.println("addData response status:{},id:{}" + response.status().getStatus() + "," + response.getId());
		client.close();
		return response.getId();
	}

	/**
	 * 通过ID删除数据
	 *
	 * @param index 索引，类似数据库
	 * @param type  类型，类似表
	 * @param id    数据ID
	 */
	public static void deleteDataById(String name, String host, String index, String type, String id) {
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(name, host);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
		client.close();
		System.out.println(
				"deleteDataById response status:{},id:{}," + response.status().getStatus() + "," + response.getId());
	}

	/**
	 * 通过ID 更新数据
	 * 
	 * @param jsonObject 要增加的数据
	 * @param index      索引，类似数据库
	 * @param type       类型，类似表
	 * @param id         数据ID
	 * @return
	 */
	public static void updateDataById(JSONObject jsonObject, String id) {
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(ElasticConfig.INDEX).type(ElasticConfig.TYPE).id(id).doc(jsonObject);
		client.update(updateRequest);
		client.close();
	}

	/**
	 * 通过ID获取数据
	 *
	 * 索引，类似数据库 类型，类似表
	 * 
	 * @param id     数据ID
	 * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
	 * @return
	 */
	public static Map<String, Object> searchDataById(String id, String fields) {

		TransportClient client = null;
		try {
			client = EsClient.getEsClient(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		GetRequestBuilder getRequestBuilder = client.prepareGet(ElasticConfig.INDEX, ElasticConfig.TYPE, id);

		if (StringUtils.isNotEmpty(fields)) {
			getRequestBuilder.setFetchSource(fields.split(","), null);
		}

		GetResponse getResponse = getRequestBuilder.execute().actionGet();
		client.close();
		// 可以将其转化为实体，通过jsonutils工具类
		// Article entity = JsonUtils.jsonToPojo(getResponse.getSourceAsString(),
		// Article.class);
		return getResponse.getSource();
	}

	/**
	 * 使用分词查询,并分页
	 *
	 * @param startPage      当前页
	 * @param pageSize       每页显示条数
	 * @param query          查询条件
	 * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortField      排序字段
	 * @param highlightField 高亮字段
	 * @return
	 */
	public static void searchDataPage(int startPage, int pageSize, QueryBuilder query, String fields,
			String sortField, String highlightField) {
		TransportClient client = null;
		try {
			client = EsClient.getEsClient(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(ElasticConfig.INDEX);
		if (StringUtils.isNotEmpty(ElasticConfig.TYPE)) { // 类型名称,可传入多个type逗号分隔
			searchRequestBuilder.setTypes(ElasticConfig.TYPE.split(","));
		}
		searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

		// 需要显示的字段，逗号分隔（缺省为全部字段）
		if (StringUtils.isNotEmpty(fields)) {
			searchRequestBuilder.setFetchSource(fields.split(","), null);
		}
		// 排序字段
		if (StringUtils.isNotEmpty(sortField)) {
			searchRequestBuilder.addSort(sortField, SortOrder.DESC);
		}
		// 高亮 好处就是加上了样式，直接返回给前端，显示就可以。
		if (StringUtils.isNotEmpty(highlightField)) {
			HighlightBuilder highlightBuilder = new HighlightBuilder();

			// highlightBuilder.preTags("<span style='color:red' >");//设置前缀
			// highlightBuilder.postTags("</span>");//设置后缀

			// 设置高亮字段
			highlightBuilder.field(highlightField);
			searchRequestBuilder.highlighter(highlightBuilder);
		}
		// searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		searchRequestBuilder.setQuery(query);
		// 分页应用
		//起始索引
		int startRows = (startPage - 1) * pageSize;
		searchRequestBuilder.setFrom(startRows).setSize(pageSize);

		// 设置是否按查询匹配度排序
		searchRequestBuilder.setExplain(true);

		// 打印的内容 可以在 Elasticsearch head 和 Kibana 上执行查询
		System.out.println(searchRequestBuilder);

		// 执行搜索,返回搜索响应信息
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

		long totalHits = searchResponse.getHits().totalHits;
		long length = searchResponse.getHits().getHits().length;

		System.out.println("共查询到" + totalHits + "条数据,处理数据条数" + length);

		if (searchResponse.status().getStatus() == 200) {
			//1. 处理方案
//			SearchHits hits = searchResponse.getHits();
//			System.out.println("结果数量：" + hits.getTotalHits());
//			// 遍历打印文档内容
//			Iterator<SearchHit> iterator = hits.iterator();
//			// 创建一个返回值对象 easyUI的
//			EasyUIDataGridResult result = new EasyUIDataGridResult();
//			List<Elastic> list = new ArrayList();
//			while (iterator.hasNext()) {
//				SearchHit hit = iterator.next();
//				Elastic pojo = JsonUtils.jsonToPojo(hit.getSourceAsString(), Elastic.class);
//				pojo.setId(hit.getId());
//				pojo.setName("deepflow");
//				list.add(pojo);
//			}
//			
			// 2. c处理方案，直接打印到控制台
			// 解析对象 可以自己封装结果集，放入list返回前端进行分页显示。比如整合easyUI的时候
			List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);
			for (Map<String, Object> map : sourceList) {
				//也可以遍历的时候直接封装到实体
				System.out.println(map.toString());
			}
			
		}

	}

	/**
	 * 高亮结果集 特殊处理
	 *
	 * @param searchResponse
	 * @param highlightField
	 */
	private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
		List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
		StringBuffer stringBuffer = new StringBuffer();
		for (SearchHit searchHit : searchResponse.getHits().getHits()) {
			searchHit.getSourceAsMap().put("id", searchHit.getId());
			if (StringUtils.isNotEmpty(highlightField)) {
				System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
				Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();
				if (text != null) {
					for (Text str : text) {
						stringBuffer.append(str.string());
					}
					//遍历 高亮结果集，覆盖 正常结果集
					searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
				}
			}
			sourceList.add(searchHit.getSourceAsMap());
		}

		return sourceList;
	}

}

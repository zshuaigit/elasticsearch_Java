package com.zshuai.elastic.springboot_elastic.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
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
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.zshuai.elastic.springboot_elastic.entity.PagingVO;

@Component
public class ElasticSearchUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchUtil.class);

	@Autowired
	private TransportClient transportClient;

	private static TransportClient client;

	/**
	 * @PostContruct是spring框架的注解 spring容器初始化的时候执行该方法
	 */
	@PostConstruct
	public void init() {
		client = this.transportClient;
	}

	/**  
	 * <p>Title: createIndex</p>  
	 * <p>Description: 创建索引</p>  
	 * @param index
	 * @return  
	 */  
	public static boolean createIndex(String index) {
		if (!isIndexExist(index)) {
			LOGGER.info("Index is not exits!");
		}
		CreateIndexResponse indexresponse = client.admin().indices().prepareCreate(index).execute().actionGet();
		LOGGER.info("创建索引成功" + indexresponse.isAcknowledged());
		return indexresponse.isAcknowledged();
	}
	/**  
	 * <p>Title: addData</p>  
	 * <p>Description: </p>  
	 * @param jsonObject 添加的数据json对象 。在调用的时候将pojo转换为jsonObject比较方便
	 * @param index		看做数据库
	 * @param type     看做表
	 * @param id
	 * @return  
	 */  
	public static String addData(JSONObject jsonObject, String index, String type, String id) {
		IndexResponse response = client.prepareIndex(index, type, id).setSource(jsonObject).get();
		LOGGER.info("添加数据 status:{},id:{}", response.status().getStatus(), response.getId());
		return response.getId();
	}
	
	/**  
	 * <p>Title: deleteDataById</p>  
	 * <p>Description: 通过ID删除数据 </p>  
	 * @param index
	 * @param type
	 * @param id  
	 */  
	public static void deleteDataById(String index, String type, String id) {

		DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();

		LOGGER.info("deleteDataById response status:{},id:{}", response.status().getStatus(), response.getId());
	}
	
	
	/**  
	 * <p>Title: isIndexExist</p>  
	 * <p>Description:判断索引是否存在 </p>  
	 * @param index
	 * @return  
	 */  
	public static boolean isIndexExist(String index) {
		LOGGER.info("测试的index名字 :{}", index);
		IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index))
				.actionGet();
		if (inExistsResponse.isExists()) {
			LOGGER.info("Index [" + index + "] is exist!");
		} else {
			LOGGER.info("Index [" + index + "] is not exist!");
		}
		return inExistsResponse.isExists();
	}
	
	/**  
	 * <p>Title: updateDataById</p>  
	 * <p>Description: 通过ID更新数据 </p>  
	 * @param jsonObject
	 * @param index
	 * @param type
	 * @param id  
	 */  
	public static void updateDataById(JSONObject jsonObject, String index, String type, String id) {

		UpdateRequest updateRequest = new UpdateRequest();

		updateRequest.index(index).type(type).id(id).doc(jsonObject);

		client.update(updateRequest);

	}
	
	/**  
	 * <p>Title: searchDataById</p>  
	 * <p>Description: 通过ID获取数据，并指定需要显示的字段 </p>  
	 * @param index
	 * @param type
	 * @param id
	 * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
	 * @return  
	 */  
	public static Map<String, Object> searchDataById(String index, String type, String id, String fields) {

		GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);

		if (StringUtils.isNotEmpty(fields)) {
			getRequestBuilder.setFetchSource(fields.split(","), null);
		}

		GetResponse getResponse = getRequestBuilder.execute().actionGet();

		return getResponse.getSource();
	}
	
	
	/**  
	 * <p>Title: searchDataPage</p>  
	 * <p>Description: 使用分词查询,并分页</p>  
	 * @param index          索引名称
	 * @param type           类型名称,可传入多个type逗号分隔
	 * @param startPage      当前页
	 * @param pageSize       每页显示条数
	 * @param query          查询条件
	 * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortField      排序字段
	 * @param highlightField 高亮字段
	 * @return  
	 */  
	public static PagingVO searchDataPage(String index, String type, int startPage, int pageSize, QueryBuilder query,
			String fields, String sortField, String highlightField) {
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
		if (StringUtils.isNotEmpty(type)) {
			searchRequestBuilder.setTypes(type.split(","));
		}
		searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

		// 需要显示的字段，逗号分隔（缺省为全部字段）
		if (StringUtils.isNotEmpty(fields)) {
			searchRequestBuilder.setFetchSource(fields.split(","), null);
		}

		//排序字段
		if (StringUtils.isNotEmpty(sortField)) {
			searchRequestBuilder.addSort(sortField, SortOrder.DESC);
		}

		// 高亮
		if (StringUtils.isNotEmpty(highlightField)) {
			HighlightBuilder highlightBuilder = new HighlightBuilder();

			// highlightBuilder.preTags("<span style='color:red' >");//设置前缀
			// highlightBuilder.postTags("</span>");//设置后缀

			// 设置高亮字段
			highlightBuilder.field(highlightField);
			searchRequestBuilder.highlighter(highlightBuilder);
		}

		//searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
		searchRequestBuilder.setQuery(query);

		// 分页应用
		int startIndex = (startPage -1)*pageSize;
		searchRequestBuilder.setFrom(startIndex).setSize(pageSize);

		// 设置是否按查询匹配度排序
		searchRequestBuilder.setExplain(true);

		// 打印的内容 可以在 Elasticsearch head 和 Kibana 上执行查询
		LOGGER.info("\n{}", searchRequestBuilder);

		// 执行搜索,返回搜索响应信息
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

		long totalHits = searchResponse.getHits().totalHits;
		long length = searchResponse.getHits().getHits().length;

		LOGGER.debug("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

		if (searchResponse.status().getStatus() == 200) {
			// 解析对象
			List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

			return new PagingVO(startPage, pageSize, (int) totalHits, sourceList);
		}

		return null;

	}

	/**  
	 * <p>Title: searchListData</p>  
	 * <p>Description: 使用分页查询</p>  
	 * @param index          索引名称
	 * @param type           类型名称,可传入多个type逗号分隔
	 * @param query          查询条件
	 * @param size           文档大小限制
	 * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortField      排序字段
	 * @param highlightField 高亮字段
	 * @return  
	 */  
	public static List<Map<String, Object>> searchListData(String index, String type, QueryBuilder query, Integer size,
			String fields, String sortField, String highlightField) {

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
		if (StringUtils.isNotEmpty(type)) {
			searchRequestBuilder.setTypes(type.split(","));
		}

		if (StringUtils.isNotEmpty(highlightField)) {
			HighlightBuilder highlightBuilder = new HighlightBuilder();
			// 设置高亮字段
			highlightBuilder.field(highlightField);
			searchRequestBuilder.highlighter(highlightBuilder);
		}

		searchRequestBuilder.setQuery(query);

		if (StringUtils.isNotEmpty(fields)) {
			searchRequestBuilder.setFetchSource(fields.split(","), null);
		}
		searchRequestBuilder.setFetchSource(true);

		if (StringUtils.isNotEmpty(sortField)) {
			searchRequestBuilder.addSort(sortField, SortOrder.DESC);
		}

		if (size != null && size > 0) {
			searchRequestBuilder.setSize(size);
		}

		// 打印的内容 可以在 Elasticsearch head 和 Kibana 上执行查询
		LOGGER.info("\n{}", searchRequestBuilder);

		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

		long totalHits = searchResponse.getHits().totalHits;
		long length = searchResponse.getHits().getHits().length;

		LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

		if (searchResponse.status().getStatus() == 200) {
			// 解析对象
			return setSearchResponse(searchResponse, highlightField);
		}
		return null;

	}

	/**  
	 * <p>Title: setSearchResponse</p>  
	 * <p>Description: 高亮结果集 特殊处理</p>  
	 * @param searchResponse
	 * @param highlightField
	 * @return  
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
	/**  
	 * <p>Title: isTypeExist</p>  
	 * <p>Description: 判断inde下指定type是否存在</p>  
	 * @param index
	 * @param type
	 * @return  
	 */  
	public boolean isTypeExist(String index, String type) {
		//boolean exists = client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists();
		return isIndexExist(index)
				? client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists()
				: false;
	}


}

package com.zshuai.elastic;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.zshuai.elastic.config.ElasticConfig;
import com.zshuai.elastic.entity.Article;
import com.zshuai.elastic.utils.ElasticUtils;

public class ElasticUtilsTest {

	@Test
	// 测试index的创建
	public void creatIndex() {
		ElasticUtils.createIndex(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST, ElasticConfig.INDEX);
	}

	@Test
	// 判断index是否存在
	public void isIndexExist() {
		boolean indexExist = ElasticUtils.isIndexExist(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST,
				ElasticConfig.INDEX);
		if (indexExist) {
			System.out.println("索引已经存在");
		} else {
			System.out.println("索引不存在");
		}
	}

	@Test
	// 删除index
	public void deleteIndex() {
		boolean deleteIndex = ElasticUtils.deleteIndex(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST,
				ElasticConfig.INDEX);
		System.out.println(deleteIndex);
	}

	@Test
	public void insertJson() {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("id", UUID.randomUUID().toString().replaceAll("-", ""));
		jsonObject.put("title", "九月九日忆山东兄弟");
		jsonObject.put("content", "独在异乡为异客，每逢佳节倍思亲。遥知兄弟登高处，遍插茱萸少一人。");
		System.out.println(jsonObject.toString());
		String id = ElasticUtils.addData(jsonObject, ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST,
				ElasticConfig.INDEX, ElasticConfig.TYPE, jsonObject.getString("id"));
		System.out.println(id);
	}

	@Test
	public void insertPojo() {
		Article pojo = new Article();
		pojo.setId(UUID.randomUUID().toString().replaceAll("-", ""));
		pojo.setTitle("静夜思");
		pojo.setContent("床前明月光，疑是地上霜。举头望明月，低头思故乡。");
		String id = ElasticUtils.insertPojo(pojo);
		System.out.println(id);
	}

	@Test
	public void deleteDataById() {
		ElasticUtils.deleteDataById(ElasticConfig.CLUSTER_NAME, ElasticConfig.HOST, ElasticConfig.INDEX,
				ElasticConfig.TYPE, "563efa32472145fb8224eca5074d0bf7");
	}

	@Test
	public void updateDataById() {
		String id = "f8cca1d6093544189552bd608a8513a4";// 上面添加的静夜思的id
		Article pojo = new Article();
		pojo.setId(id);
		pojo.setTitle("静夜思（修改）");
		pojo.setContent("床前明月光，疑是地上霜。举头望明月，低头思故乡。");
		JSONObject jsonObject = (JSONObject) JSONObject.toJSON(pojo);
		ElasticUtils.updateDataById(jsonObject, id);
	}

	@Test
	public void getData() {
		String id = "f8cca1d6093544189552bd608a8513a4";// 上面添加的静夜思的id
		if (StringUtils.isNotBlank(id)) {
			Map<String, Object> map = ElasticUtils.searchDataById(id, null);
			System.out.println(JSONObject.toJSONString(map));
		}
	}

	@Test
	public void searchDataPage() {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//      boolQuery.must(QueryBuilders.rangeQuery("date").from("2018-04-25T08:33:44.840Z")
//      .to("2019-04-25T10:03:08.081Z")); 区间查询
		ElasticUtils.searchDataPage(2, 3, boolQuery, null, null, null);
	}

}

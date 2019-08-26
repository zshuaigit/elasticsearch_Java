package com.zshuai.elastic.springboot_elastic.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @NoArgsConstructor: 自动生成无参数构造函数。 
 * @AllArgsConstructor: 自动生成全参数构造函数。 
 * @Data: 自动为所有字段添加@ToString, @EqualsAndHashCode, @Getter方法，为非final字段添加@Setter,和@RequiredArgsConstructor
 * @author zshuai
 * @date Aug 26, 2019
 */
@Data
@ToString
@NoArgsConstructor
public class DemoEntity {

	private String id;
	
	private String name;
	
	private int age;
	
	private String timestamp;
}

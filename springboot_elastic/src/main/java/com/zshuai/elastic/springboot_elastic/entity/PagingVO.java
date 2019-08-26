package com.zshuai.elastic.springboot_elastic.entity;

import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.ToString;

/**
 * @Data: 自动为所有字段添加@ToString, @EqualsAndHashCode, @Getter方法，为非final字段添加@Setter,和@RequiredArgsConstructor
 */
@Data
@ToString
public class PagingVO {
	private int currentPage;// 当前页
	
	private int pageSize;// 每页显示多少条

	private int recordCount;// 总记录数
	private List<Map<String, Object>> recordList;// 本页的数据列表(可以将其泛型设置为相应的实体)

	private int pageCount;// 总页数
	
	private int beginPageIndex;// 页码列表的开始索引（包含）
	
	private int endPageIndex;// 页码列表的结束索引（包含）

    public PagingVO(int currentPage, int pageSize, int recordCount, List<Map<String, Object>> recordList) {
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.recordCount = recordCount;
        this.recordList = recordList;

        // 计算总页码(一些前端框架只需要给其数据总条数，会自动计算总页数)
        pageCount = (recordCount + pageSize - 1) / pageSize;

        // 计算 beginPageIndex 和 endPageIndex
        // >> 总页数不多于10页，则全部显示
        if (pageCount <= 10) {
            beginPageIndex = 1;
            endPageIndex = pageCount;
        }
        // 总页数多于10页，则显示当前页附近的共10个页码
        else {
            // 当前页附近的共10个页码（前4个 + 当前页 + 后5个）
            beginPageIndex = currentPage - 4;
            endPageIndex = currentPage + 5;
            // 当前面的页码不足4个时，则显示前10个页码
            if (beginPageIndex < 1) {
                beginPageIndex = 1;
                endPageIndex = 10;
            }
            // 当后面的页码不足5个时，则显示后10个页码
            if (endPageIndex > pageCount) {
                endPageIndex = pageCount;
                beginPageIndex = pageCount - 10 + 1;
            }
        }
    }
}
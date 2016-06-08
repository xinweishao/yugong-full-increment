package com.taobao.yugong.translator;

import java.util.List;

import com.taobao.yugong.common.model.record.Record;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public interface DataTranslator {

    /**
     * 静态转换schemaName的逻辑,对于所有数据的默认转换,也允许基于每条数据动态计算schema,如果为null则忽略转换处理,代表不做转换
     */
    public String translatorSchema();

    /**
     * 静态转换tableName的逻辑,对于所有数据的默认转换,也允许基于每条数据动态计算table,如果为null则忽略转换处理,代表不做转换
     */
    public String translatorTable();

    // ==================

    /**
     * 单条记录处理，返回true代表需要同步，返回false需要忽略，如果对于源数据有修改，直接修改对应{@linkplain Record}的属性值
     * 
     * @param record
     * @return
     */
    public boolean translator(Record record);

    /**
     * 多条记录处理，返回为处理过后的记录. 输入/输出记录数可不相同，以输出为准
     * 
     * @param records
     * @return
     */
    public List<Record> translator(List<Record> records);
}

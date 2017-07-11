/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 manticorecao@gmail.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */


package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 处理复合索引的Translator
 * @author caobin
 * @version 1.0 2017.07.07
 * <p>
 *
 *     - class: com.taobao.yugong.translator.CompositeIndexesDataTranslator
 *       properties:
 *       # # sqlserver field
 *       composite_indexes:
 *       - OrderID
 *       - ProductID
 *       - MultiProductID
 *
 * </p>
 */

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@Data
public class CompositeIndexesDataTranslator extends AbstractDataTranslator {

    private List<String> compositeIndexes = new ArrayList<>();

    public CompositeIndexesDataTranslator() {
        ColumnTranslator translator = new ColumnTranslator();
        super.setTranslator(translator);
    }

    @Override
    public boolean translator(Record record) {

        if(!compositeIndexes.isEmpty()){
            //标记Record为联合索引处理
            record.setEnableCompositeIndexes(true);
            //清空原有主键
            List<ColumnValue> primaryKeys = record.getPrimaryKeys();
            primaryKeys.clear();
            //重设索引键
            compositeIndexes.stream().forEach(k -> {
                ColumnValue columnValue = record.getColumnByName(k);
                primaryKeys.add(columnValue);
                record.getColumns().remove(columnValue);
                //for check mode only
                record.addCheckCompositeKey(k);
            });
        }

        return super.translator(record);
    }
}

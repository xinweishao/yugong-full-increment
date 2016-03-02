package com.taobao.yugong.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.stats.ProgressTracer;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public abstract class AbstractRecordExtractor extends AbstractYuGongLifeCycle implements RecordExtractor {

    protected final Logger            logger = LoggerFactory.getLogger(this.getClass());

    protected volatile ExtractStatus  status = ExtractStatus.NORMAL;

    protected volatile ProgressTracer tracer;

    public void setStatus(ExtractStatus status) {
        this.status = status;
    }

    public ExtractStatus status() {
        return status;
    }

    public ProgressTracer getTracer() {
        return tracer;
    }

    public void setTracer(ProgressTracer tracer) {
        this.tracer = tracer;
    }

}

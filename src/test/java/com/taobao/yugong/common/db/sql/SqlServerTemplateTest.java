package com.taobao.yugong.common.db.sql;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SqlServerTemplateTest {
  @Test
  public void getInsertSql() throws Exception {
    SqlServerTemplate template = new SqlServerTemplate();
    String[] primaryKeys =  {"i1"};
    String[] columns =  {"c1", "c2"};
    String sql = template.getInsertSql("s", "user", primaryKeys, columns);
    Assert.assertEquals("SET IDENTITY_INSERT user ON;INSERT INTO dbo.user(c1 , c2 , i1) VALUES "
        + "(? , ? , ?)", sql);
  }

}
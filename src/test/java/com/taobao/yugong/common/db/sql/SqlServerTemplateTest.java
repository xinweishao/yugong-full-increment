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

  @Test
  public void getMergeSql() throws Exception {
    SqlServerTemplate template = new SqlServerTemplate();
    String[] primaryKeys =  {"pk1"};
    String[] columns =  {"c1", "c2", "c3"};
    String sql = template.getMergeSql("sss", "ttt", primaryKeys, columns);
    Assert.assertEquals("MERGE ttt AS target\n"
        + "USING (values (?, ?, ?, ?)) AS source (c1, c2, c3, pk1)\n"
        + "ON target.pk1 = source.pk1\n"
        + "WHEN MATCHED THEN\n"
        + "   UPDATE SET c1 = source.c1, c2 = source.c2, c3 = source.c3\n"
        + "WHEN NOT MATCHED THEN\n"
        + "   INSERT (c1, c2, c3, pk1) VALUES (source.c1, source.c2, source.c3, source.pk1);", sql);
  }

  @Test
  public void getMergeSql2() throws Exception {
    SqlServerTemplate template = new SqlServerTemplate();
    String[] primaryKeys =  {"pk1", "pk2"};
    String[] columns =  {"c1", "c2", "c3"};
    String sql = template.getMergeSql("sss", "ttt", primaryKeys, columns);
    Assert.assertEquals("MERGE ttt AS target\n"
        + "USING (values (?, ?, ?, ?, ?)) AS source (c1, c2, c3, pk1, pk2)\n"
        + "ON target.pk1 = source.pk1 AND target.pk2 = source.pk2\n"
        + "WHEN MATCHED THEN\n"
        + "   UPDATE SET c1 = source.c1, c2 = source.c2, c3 = source.c3\n"
        + "WHEN NOT MATCHED THEN\n"
        + "   INSERT (c1, c2, c3, pk1, pk2) VALUES (source.c1, source.c2, source.c3, source.pk1, source.pk2);", sql);
  }
}
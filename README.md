<h1>阿里巴巴去Oralce数据迁移同步工具(目标支持MySQL/DRDS)</h1>
----
<h1>背景</h1>
<p>&nbsp; &nbsp;08年左右，阿里巴巴开始尝试MySQL的相关研究，并开发了基于MySQL分库分表技术的相关产品，Cobar/TDDL(目前为阿里云DRDS产品)，解决了单机Oracle无法满足的扩展性问题，当时也掀起一股去IOE项目的浪潮，愚公这项目因此而诞生，其要解决的目标就是帮助用户完成从Oracle数据迁移到MySQL上，完成去IOE的第一步.&nbsp;</p>
<h1>项目介绍</h1>
<p>名称: &nbsp; yugong</p>
<p>译意: &nbsp; 愚公移山</p>
<p>语言: &nbsp;&nbsp;纯java开发</p>
<p>定位: &nbsp; 数据库迁移 (目前主要支持oracle -&gt; mysql/DRDS)</p>
<h1>项目介绍</h1>
<p style="box-sizing: border-box; margin-bottom: 16px; color: #333333; font-family: 'Helvetica Neue', Helvetica, 'Segoe UI', Arial, freesans, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol'; font-size: 16px; line-height: 25.6px;">整个数据迁移过程，分为两部分：</p>
<ol>
<li>全量迁移</li>
<li>增量迁移</li>
</ol>
<p><img src="https://camo.githubusercontent.com/9a9cc09c5a7598239da20433857be61c54481b9c/687474703a2f2f646c322e69746579652e636f6d2f75706c6f61642f6174746163686d656e742f303131352f343531312f31306334666134632d626634342d333165352d623531312d6231393736643164373636392e706e67" alt="" /></p>
<p>过程描述：</p>
<ol>
<li>增量数据收集 (创建oracle表的增量物化视图)</li>
<li>进行全量复制</li>
<li>进行增量复制 (可并行进行数据校验)</li>
<li>原库停写，切到新库</li>
</ol>
<h1>架构</h1>
<p><img src="http://dl2.iteye.com/upload/attachment/0115/5467/aae71c94-5873-31b1-a6c8-fb605a9f2319.png" alt="" width="584" height="206" /></p>
<p>说明:&nbsp;</p>
<ol>
<li><!--StartFragment-->
<div style="margin-top: 10.8pt; margin-bottom: 0pt; direction: ltr; unicode-bidi: embed; vertical-align: baseline;">一个Jvm Container对应多个instance，每个instance对应于一张表的迁移任务</div>
<!--EndFragment--></li>
<li><!--StartFragment-->&nbsp;instance分为三部分<br />a.&nbsp; <!--StartFragment-->extractor &nbsp;(从源数据库上提取数据，可分为全量/增量实现)<!--EndFragment--><br />b.&nbsp; <!--StartFragment-->translator &nbsp;(将源库上的数据按照目标库的需求进行自定义转化)<!--EndFragment--><br />c. &nbsp;applier<!--EndFragment-->&nbsp; (将数据更新到目标库，可分为全量/增量/对比的实现<!--EndFragment-->)</li>
</ol>
<h1>DevDesign</h1>
<p>See the page for dev design: [[DevDesign]]</p>
<h1>QuickStart</h1>
<p>See the page for quick start: &nbsp;[[QuickStart]]</p>
<h1>AdminGuide</h1>
<p>See the page for admin deploy guide: [[AdminGuide]]</p>
<h1>Performance</h1>
<p>See the page for yugong performance : [[Performance]]</p>
<h1>相关资料</h1>
<ol>
<li>yugong简单介绍ppt : &nbsp;<a href="https://github.com/alibaba/yugong/blob/master/docs/yugong_Intro.ppt?raw=true">ppt</a></li>
<li><a href="https://www.aliyun.com/product/drds">分布式关系型数据库服务DRDS</a> (前身为阿里巴巴公司的Cobar/TDDL的演进版本, 基本原理为MySQL分库分表)</li>
</ol>
<h1>问题反馈</h1>
<ol>
<li><span style="line-height: 21px;">qq交流群： 537157866</span></li>
<li><span style="line-height: 21px;">邮件交流： jianghang115@gmail.com</span></li>
<li><span style="line-height: 21px;">新浪微博： agapple0002</span></li>
<li><span style="line-height: 21px;">报告issue：<a href="https://github.com/alibaba/yugong/issues">issues</a></span></li>
</ol>
package com.yalong.gmall01.realtime.utils

import java.util
import java.util.Properties

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder


/**
 * @auther levithanlee
 * @ccreate${year}-${month}-${day}${time}
 */
object MyEsUtil {
      private   var   factory: JestClientFactory=null;

//构建工厂
  def getclient:JestClient={
    if (factory == null) build()
    factory.getObject

  }
  def build():Unit = {
    factory = new JestClientFactory
    //写入ES地址
    val properties: Properties = PropertiesUtil.load("config.properties")
    val serverUri: String = properties.getProperty("elasticsearch.server")
    factory.setHttpClientConfig(new HttpClientConfig.Builder(serverUri)
      .multiThreaded(true)
      .connTimeout(10000)
        .readTimeout(10000).build())

  }
  /*  (写操作)  val jest: JestClient = getclient
      //source可以放 2中 1 case class 2通用可装json对象，map
      val index = new Index.Builder(MovieTest("0104","速度与激情")).index("movie_test0921_20210126").`type`("_doc").build()
      jest.execute(index)
      jest.close()*/

  def main(args: Array[String]): Unit = {
    search()

  }
  def  search():Unit = {
    val jest: JestClient = getclient
    //组织参数
    //方式1
    //val query="{\n  \"query\":{\n    \"match\": {\"name\":\"operation red sea\"}\n  }\n  , \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ],\n  \"size\":2\n  , \"from\":0\n  ,\"_source\":[\"name\",\"doubanScore\"]\n  ,\"highlight\": {\"fields\": {\"name\":{\"pre_tags\": \"<span color='red'>\",\"post_tags\":\"</span>\"}}}\n}"
    //方式2 更优美
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(new MatchQueryBuilder("name","operation red sea"))
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)
    searchSourceBuilder.size(2)
    searchSourceBuilder.from(0)
    searchSourceBuilder.fetchSource(Array("name","doubanScore"),null)
    searchSourceBuilder.highlighter(new HighlightBuilder()
      .field("name").preTags("<span>").postTags("</span>"))

    val search1: Search = new Search.Builder(searchSourceBuilder.toString)
      .addIndex("movie_index0921").addType("movie").build()

    val result: SearchResult = jest.execute(search1)

    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    //包装容器，1 专用容器，case class 2 通用容器 ,map
    import  collection.JavaConverters._
    for ( rs <- rsList.asScala ) {
      println(rs.source)
    }

    //接受结果
    jest.close()

  }
  val DEFAULT_TYPE = "_doc"
  //批量保存
  def saveBulk(indexName:String, docList:List[(String,Any)]):Unit={
    val jest: JestClient = getclient  //通用

    val bulkBuilder = new Bulk.Builder()
    //加入很多个单行操作,需要考虑幂等性问题，在每条数据后加上id字段 ,保证唯一性
    for ( (id, doc) <- docList ) {
      val index = new Index.Builder(doc).id(id).build()
      bulkBuilder.addAction(index)
    }
    //提取出来这2个重复的，加入统一索引
    bulkBuilder.defaultIndex(indexName).defaultType(DEFAULT_TYPE)
    val bulk: Bulk = bulkBuilder.build()
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
    println("已经保存了："+ items.size()+ "条")


    jest.close()


  }

def save():Unit ={
  //（读操作）
  val jest: JestClient = getclient
  //source可以放 2中 1 case class 2通用可装json对象，map
  val index = new Index.Builder(MovieTest("0104","速度与激情")).index("movie_test0921_20210126").`type`("_doc").build()
  jest.execute(index)
  jest.close()
}




  case class MovieTest(id: String,movie_name:String)

}

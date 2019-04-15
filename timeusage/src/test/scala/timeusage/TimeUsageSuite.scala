/*package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random



@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  def initializeTimeUsage(): Boolean =
    try {
      TimeUsage
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

     override def afterAll(): Unit = {
    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
    import TimeUsage._
    spark.sparkContext.stop()
  }

      val summed =   
     Seq(TimeUsageRow("working", "female", "active", 7.667, 0.62, 5.153),
       TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
       TimeUsageRow("working", "female", "active", 7.667, 0.62, 5.153),
       TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
       TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
       TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
       TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
       TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
       TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
       TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
       TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
       TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
       TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
       TimeUsageRow("not working", "male", "young", 4.42, 0.53, 5.7),
       TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111),
       TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
       TimeUsageRow("working", "female", "active", 7.667, 0.62, 5.153),
       TimeUsageRow("working", "female", "young", 13.9501, 8.97, 0.0444),
       TimeUsageRow("working", "male", "elder", 11.002, 2.99999, 5.7111)
     ).toDF
     
      test("'dfSchema' should work for (specific) RDD with one element") {
    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
    import TimeUsage._
    //val rdd = spark.sparkContext.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val struttype= dfSchema(Seq("col1","Col2","Col3", "Col4").toList)
    val res =(struttype.fields.length == 4)
    assert(res, "dfschema should return StructType with four fields")
  }
      
      test("'classifiedColumn' should work for list of columns") {
    assert(initializeTimeUsage(), " -- did you fill in all the values in TimeUsage (conf, sc, wikiRdd)?")
    import TimeUsage._
    //val rdd = spark.sparkContext.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val cols: (List[Column], List[Column], List[Column])= classifiedColumns(Seq("t01","t03","t04","t06","t05","t1801", "t1802","t1805").toList)
    val res =(cols._1.size == 3) && (cols._2.size == 2) && (cols._3.size == 3)
      
    assert(res, "Columns are not classified correctly")
    
    
  }
      
          test("'timeusagesummary' should work for list of columns") {
  
    import TimeUsage._
    //val rdd = spark.sparkContext.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val struttype= dfSchema(Seq("col1","Col2","Col3", "Col4").toList)
    val res =(struttype.fields.length == 4)
    assert(res, "dfschema should return StructType with four fields")
  }
      
      
}
*/
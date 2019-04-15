/*

package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.enablers.Length
import org.apache.spark.rdd.RDD


*//** Main class *//*

object Sample {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Sample")
      .config("spark.master", "local").config("spark.sql.optimizer.maxIterations", 500)
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  case class Employee(id: Int, name: String, age: Int, gender:String)
  val sc = spark.sparkContext
  

  *//** Main function *//*
  def main(args: Array[String]): Unit = {//val empDF = createDFFromList();
    val empDF = createDFFromCSV();
 val workingStatusProjection: Column = (when(empDF("telfs")>=1 && empDF("telfs")<3, "working") otherwise("not working")).as("working")
    //df.col("telfs").as("workingStatus")
    val sexProjection: Column =  (when(empDF("tesex")===1, "male") otherwise("female")).as("sex")
    val ageProjection: Column =(when(empDF("teage")>=15 && empDF("teage")<=55, when(empDF("teage")<=22, "young") otherwise "active") otherwise("adult")).as("age")
    
 val (columns, initDf) = TimeUsage.read("/timeusage/atussum.csv")

    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)
//val cols: List[Column] = List(empDF("gemetsta"), empDF("peeduca"))
 val primaryNeedsProjection: Column = primaryNeedsColumns.reduce((a,b)=>((a+b)/60)).as("primaryNeedsProjection")
    val workProjection: Column =workColumns.reduce((a,b)=>((a+b)/60)).as("workProjection")
    println("otherColumns"+ otherColumns)
   val otherProjection: Column = otherColumns.reduce((a,b)=>((a+b)/60)).as("otherProjection")
  val summary =  empDF
      .select(workingStatusProjection, sexProjection, ageProjection,primaryNeedsProjection,workProjection,otherProjection)
      .where($"telfs" <= 4)
      
    val avgDF  =  summary.groupBy("working", "sex","age").agg(avg($"primaryNeedsProjection"),avg($"workProjection"),avg($"otherProjection"))
    
    
    avgDF.show()
    
     val ds = summary.as("TimeUsageRow")
    var summed = ds.map(row=>TimeUsageRow(
                                                              row.getAs[String]("working"),
                                                              row.getAs[String]("sex"),
                                                              row.getAs[String]("age"),
                                                              row.getAs[Double]("primaryNeedsProjection"),
                                                              row.getAs[Double]("workProjection"),
                                                              row.getAs[Double]("otherProjection")
                                                              ))
                                                              
  var groupTyped = summed.groupBy("working", "sex","age").agg(avg($"primaryNeeds").as[Double],avg($"work").as[Double],avg($"other").as[Double])
   
   
   
   avg("primaryNeedsProjection","workProjection","otherProjection").map(row=>TimeUsageRow(
                                                              row.getAs[String]("working"),
                                                              row.getAs[String]("sex"),
                                                              row.getAs[String]("age"),
                                                              row.getAs[Double]("primaryNeedsProjection"),
                                                              row.getAs[Double]("workProjection"),
                                                              row.getAs[Double]("otherProjection")
                                                              )).show()
    
    
     // summary.groupBy("working").avg($"age").show()

//val sumDf =when( empDF("sum"), empDF.withColumn("sum", cols.reduce((a,b)=>(a+b)/60)))

  empDF .select(workingStatusProjection,sexProjection,ageProjection,sum).where($"telfs" <= 4).show()
     // println("working " + sum.take(1)(0))


  
//  value.count();
   val  df= empDF.rdd.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
    
    
  }

  
  
  
   def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    var primary: List[Column] = List()
    var work: List[Column] = List()
    var other: List[Column] = List()
    var primaryRegex= "(t01|t03|t11|t1801|t1803).*".r
    var workRegex = "(t05|t1805).*".r
    var otherReqex="(t02|t04|t06|t07|t08|t09|t10|t12|t13|t14|t15|16|t18).*".r
    
    for{
      name <- columnNames
    } {
      name match {
        case primaryRegex(m) => primary= col(name) :: primary 
           case workRegex(m) => work = col(name) :: work 
      case otherReqex(m) => other = col(name) :: other
      case _=> print("No match")
      }
    
    }
 
    
    (primary,work,other)
  }
  
  def createDFFromList(): DataFrame = {
         import spark.implicits._

    val empList = Seq(Employee(1, "name", 21,"M"), Employee(2, "name2", 22,"M"), Employee(3, "name3", 23,"M"), Employee(4, "name4", 24,"M"), Employee(5, "name5", 25,"M"))
    spark.sparkContext.parallelize(empList).toDF("id", "name", "age")
  }

  def createDFFromCSV(): DataFrame = {
    val rdd = sc.textFile("C:/Monika/workspaces/scalaspark/timeusage/src/main/resources/timeusage/test.csv")
    // Compute the schema based on the first line of the CSV file
    var schema: StructType = StructType(StructField("id", IntegerType, false) :: StructField("name", StringType, false) :: StructField("age", IntegerType, false):: List(StructField("gender", StringType, false)))

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    spark.createDataFrame(data, schema)

  }
  
  
  def createDFFromCSV(): DataFrame = {
    val rdd = sc.textFile("C:/Monika/workspaces/scalaspark/timeusage/src/main/resources/timeusage/abc.csv")
    // Compute the schema based on the first line of the CSV file
    var schema: StructType = dfSchema(rdd.first().split(",").to[List])

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    spark.createDataFrame(data, schema)

  }
  
   def dfSchema(columnNames: List[String]): StructType ={
     var field: StructField = StructField(columnNames(0), StringType, false)
    var fields: List[StructField] = List(field)
    for (i <- 1 until columnNames.length) {
       field = StructField(columnNames(i), DoubleType, false)

     fields= fields :+ field
    }
    StructType(fields)
  }
   
   def row(line: List[String]): Row = {
     var seq: Seq[Any] = Seq(line(0).asInstanceOf[String])
     
    val numOtherCols = line.length-1
   for(i <- 1 to numOtherCols){
     //println("col"+ line(i))
     seq=  seq :+ line(i).toDouble
   }
        Row.fromSeq(seq)


}
}*/
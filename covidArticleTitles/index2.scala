// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.
import spark.implicits._

case class Article(title: String, publish_time: String, journal: String)

val caseClassDS = Seq(Article("Hello World.", "2020-01-02", "An article about something.")).toDS()
caseClassDS.show()

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val path = "articles.json"
val articles = sqlContext.read.json(path)
val articleDs = articles.as[Article]
articleDs.show()
// val selection = articleDs.
// Register this DataFrame as a table.
// articles.registerTempTable("articles")
// val reviewTexts = sqlContext.sql("SELECT title FROM articles")
// val reviewTextsRdd = reviewTexts.rdd
// reviewTextsRdd.saveAsTextFile("output")
// val counts = reviewTextsRdd.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
// counts.saveAsTextFile("output")

//title abstract publisht_time
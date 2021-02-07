import spark.implicits._

// Define the Article class
case class Article(title: String, publish_time: String, journal: String)

// Demonstrate the Article Class
val caseClassDS = Seq(Article("Hello World.", "2020-01-02", "An article about something.")).toDS()
caseClassDS.show()

// Define SQLContext, location of the file to read, and read the JSON file
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val path = "articles.json"
val articles = sqlContext.read.json(path)

// Register this DataFrame as a table and run a SELECT query
articles.registerTempTable("articles")
val reviewTexts = sqlContext.sql("SELECT title FROM articles")

// Convert the temp table to an RDD, then save as text file
val reviewTextsRdd = reviewTexts.rdd
reviewTextsRdd.saveAsTextFile("output")

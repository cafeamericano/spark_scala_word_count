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

// Using the Article class, turn 'articles' into a dataset
val articleDs = articles.as[Article]

// Print to the console a visual of the article dataset
articleDs.show()

articleDs.write.format("json").save("jsonFile")
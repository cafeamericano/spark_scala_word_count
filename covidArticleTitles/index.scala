// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val path = "articles.json"
val people = sqlContext.read.json(path)
// Register this DataFrame as a table.
people.registerTempTable("articles")
val reviewTexts = sqlContext.sql("SELECT title FROM articles")
val reviewTextsRdd = reviewTexts.rdd
reviewTextsRdd.saveAsTextFile("output")

val text = sc.textFile("input.txt")
val counts = text.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_);
counts.saveAsTextFile("output")
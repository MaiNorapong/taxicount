javac -d target/classes PairWritable.java TaxiCountLoc.java TaxiCountDay.java
jar -cvf ~/taxicount/target/taxicount.jar -C classes/ .

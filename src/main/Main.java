package main;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import fileservices.ReadFileText;
import fileservices.WriteFileParquet;
import model.ModelLog;

public class Main {
	public static void writeParquetFile() throws IOException, ParseException {
		System.out.println("START PROCESSING........");

		ReadFileText reader = new ReadFileText();
		List<ModelLog> listModelLog = reader.getListModelLog();

		for (ModelLog model : listModelLog) {
			System.out.println(model);
		}

		WriteFileParquet writer = new WriteFileParquet(listModelLog);
		writer.writeDataToPath();
		
		System.out.println("END PROCESSING.....");
	}
	
	public static void main(String[] args) throws IOException, ParseException{
		// write parquet file to HFDS, edit path input file text in Object ReadFileText.java
//		writeParquetFile();
		
		SparkSession spark = SparkSession.builder().appName("Read file parquet to HDFS").master("local").getOrCreate();
		Dataset<Row> parquetFile = spark.read().parquet(
				"hdfs://127.0.0.1:9000/usr/trannguyenhan/pageviewlog");
		
		parquetFile.printSchema();
		
		parquetFile.createOrReplaceTempView("data");
		spark.sql("select cId, count(locID) from data group by cID").show();
	}
}

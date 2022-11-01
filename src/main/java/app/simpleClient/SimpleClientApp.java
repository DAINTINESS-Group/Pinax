package app.simpleClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.opencsv.exceptions.CsvException;

import engine.SchemaManagerInterface;
import engine.SchemaManagerFactory;
import model.StructuredFile;
import querymanager.QueryManagerFactory;
import querymanager.QueryManagerInterface;

public class SimpleClientApp {
	
	
	// WE DONOT WANT A MAIN WITH INTERACTION. WILL HAVE A GUI FOR THIS
	//FOR THE MOMENT WE NEED A SIMPLE CLIENT THAT MAKES BACK-END CALLS VIA THE INTERFACES
	//KIND-LIKE-A TEST
	public static void main(String[] args) throws AnalysisException, IOException, CsvException {
		SparkSession spark = SparkSession
				.builder()
				.appName("A simple client to do things with Spark ")
				.config("spark.master", "local")
				.getOrCreate();
		Logger rootLogger = Logger.getRootLogger(); //remove the messages of spark
		rootLogger.setLevel(Level.ERROR);
		
		SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		QueryManagerInterface qrMan = new QueryManagerFactory().createQueryManager();
	
		Dataset<Row> df = null;
		
		schMan.wipeRepoFile();
		schMan.wipeFileList();
		
		String[] filePaths = {"src/main/resources/more_stats.tsv",
								"src/main/resources/try.tsv", 
								"src/main/resources/person.tsv", 
								"src/main/resources/joomlatools__joomla-platform-categories.tsv"};
		for(String s: filePaths) {
			Path path = Paths.get(s);
			String fileAlias = schMan.createFileAlias(path.getFileName().toString());
			String fileType = schMan.getFileType(path.getFileName().toString());
			schMan.registerFileAsDataSource(fileAlias, path, fileType);
		}
		
		List<StructuredFile> fileList = schMan.getFileList();
		System.out.println("------------------------------------");
		for(StructuredFile sf: fileList) {
			if(sf.getSfType().equals("tsv")) {
				df = spark.read().option("delimiter", "\t").option("header", "true").option("inferSchema","true").csv(sf.getSfPath().toRealPath().toString());
				df.createGlobalTempView(sf.getSfAlias());
				System.out.println(sf.getSfAlias());
			}
			else if(sf.getSfType().equals("csv")) {
				df = spark.read().option("delimiter", ",").option("header", "true").option("inferSchema","true").csv(sf.getSfPath().toRealPath().toString());
				df.createGlobalTempView(sf.getSfAlias());
				System.out.println(sf.getSfAlias());
			}
		}
		System.out.println("------------------------------------");
	
		String naiveQueryExpression = qrMan.createNaiveQueryExpression("more_stats");
		spark.sql(naiveQueryExpression).show((int)df.count(),false);
		
		List<String> listOfAttributes = new ArrayList<String>();
		listOfAttributes.add("first_name");
		listOfAttributes.add("last_name");
		listOfAttributes.add("day");
		String projectionOnlyQueryExpression = qrMan.createProjectionOnlyQueryExpression("more_stats", listOfAttributes);
		spark.sql(projectionOnlyQueryExpression).show((int)df.count(),false);
		
		List<String> anotherListOfAttributes = new ArrayList<String>();
		anotherListOfAttributes.add("first_name");
		anotherListOfAttributes.add("last_name");
		anotherListOfAttributes.add("day");
		String filters = " stats.last_name = 'kon' and first_name = 'stelios'";
		String projectSelectSingleTableQueryExpression = qrMan.createProjectSelectSingleTableQueryExpression("more_stats", "stats", anotherListOfAttributes, filters);
		System.out.println(projectSelectSingleTableQueryExpression);
		spark.sql(projectSelectSingleTableQueryExpression).show((int)df.count(),false);
		
		
		String primaryTable = "more_stats";
		List<String> joinTables = new ArrayList<String>();
		joinTables.add("person");
		List<String> tableAliases = new ArrayList<String>();
		tableAliases.add("m");
		tableAliases.add("p");
		List<String> attributeNames = new ArrayList<String>();
		attributeNames.add("p.first_name");
		attributeNames.add("p.last_name");
		attributeNames.add("day");
		attributeNames.add("m.first_name");
		attributeNames.add("m.last_name");
		List<String> joinFilters = new ArrayList<String>();
		joinFilters.add("p.ip_address = m.ip_address");
		List<String> joinTypes = new ArrayList<String>();
		joinTypes.add("LEFT");
		String whereFilter = "p.gender = 'Male'";
		
		String multiTableQueryExpression = qrMan.createMultiTableQueryExpression(primaryTable,joinTables,tableAliases,attributeNames,joinFilters,joinTypes,whereFilter);
		System.out.println(multiTableQueryExpression);
		spark.sql(multiTableQueryExpression).show((int)df.count(),false);
		
		//Dataset<Row> df = spark.read().option("delimiter", "\t").option("header", "true").option("inferSchema","true").csv(path.toRealPath().toString());
		//df = spark.read().option("delimiter", ",").option("header", "true").option("inferSchema","true").csv();
		//ignore any front-end mambo jumbo. do it programmatically in the client.
		//try some stuff to see if it works
		spark.stop();
		System.out.println("You exited the program");
	}
}
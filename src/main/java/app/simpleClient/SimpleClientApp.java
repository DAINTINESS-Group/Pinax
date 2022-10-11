package app.simpleClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.opencsv.exceptions.CsvException;

import engine.SchemaManager;
import model.StructuredFile;
import querymanager.QueryManager;

public class SimpleClientApp {
	public static void main(String[] args) throws AnalysisException, IOException, CsvException {
		SparkSession spark = SparkSession
				.builder()
				.appName("A simple client to do things with Spark ")
				.config("spark.master", "local")
				.getOrCreate();
		Logger rootLogger = Logger.getRootLogger(); //remove the messages of spark
		rootLogger.setLevel(Level.ERROR);
		
		SchemaManager schMan = new SchemaManager();
		QueryManager qrMan = new QueryManager();
		boolean stopFlag = false;
		Scanner scanner = new Scanner(System.in);
		int counter = 0;
		Dataset<Row> df = null;
		while(stopFlag == false) {
			System.out.println("give an integer to do someting.\n"
					+"1 is used to wipe repo and file list\n"
					+"2 is used to register a file\n" 
					+"3 is used to create dfs for all the files in file list\n"
<<<<<<< HEAD
					+"4 is used to list all the dfs in the repo\n"
=======
>>>>>>> e96841040cee99bd5f20fba0421db1e8fdd9815a
					+"0 is used to exit");
			int code = scanner.nextInt();
			//wipe all the contents of the Repo file and file list.
			if(code == 1) {
				System.out.println("Are you sure you want to wipe the Repo? If yes then type 0 else type any Int");
				Integer flag = scanner.nextInt();
				if(flag == 0) {
					schMan.wipeRepoFile();
					schMan.wipeFileList();
				}
				else {
					System.out.println("Phew");
				}
			}
			//register the file
			else if(code == 2) {
				@SuppressWarnings("resource")
				Scanner scanner1 = new Scanner(System.in);
				System.out.println("Enter the File path");
				String filePath = scanner1.next();
				Path path = Paths.get(filePath);
				String fileAlias = schMan.createFileAlias(path.getFileName().toString());
				String fileType = schMan.getFileType(path.getFileName().toString());
				schMan.registerFileAsDataSource(fileAlias, path, fileType);
			}
			else if(code == 3) {
				List<StructuredFile> fileList = schMan.getFileList();
				if(counter == 0) {
					System.out.println("To avoid getting confused wait until the names of the files are printed");
					for(StructuredFile sf: fileList) {
						if(sf.getSfType().equals("tsv")) {
							df = spark.read().option("delimiter", "\t").option("header", "true").option("inferSchema","true").csv(sf.getSfPath().toRealPath().toString());
							df.createGlobalTempView(sf.getSfAlias());
						}
						System.out.println(sf.getSfAlias());
					}
					counter ++;
				}
				else {
					for(StructuredFile sf: fileList) {
						System.out.println(sf.getSfAlias());
					}
				}
				String queryExpression = qrMan.createQueryExpression();
				spark.sql(queryExpression).show((int)df.count(),false);
			}
<<<<<<< HEAD
			else if(code == 4) {
				List<StructuredFile> fileList = schMan.getFileList();
				System.out.println("------------------------------------");
				for(StructuredFile sf: fileList) {
					System.out.println(sf.getSfAlias());
				}
				System.out.println("------------------------------------");
			}
=======
>>>>>>> e96841040cee99bd5f20fba0421db1e8fdd9815a
			//end the loop
			else if(code == 0) {
				scanner.close();
				stopFlag = true;
			}
		}
		//Dataset<Row> df = spark.read().option("delimiter", "\t").option("header", "true").option("inferSchema","true").csv(path.toRealPath().toString());
		//df = spark.read().option("delimiter", ",").option("header", "true").option("inferSchema","true").csv();
		//ignore any front-end mambo jumbo. do it programmatically in the client.
		//try some stuff to see if it works
		spark.stop();
		System.out.println("You exited the program");
		//etc.
		
	}//end main
}//end class

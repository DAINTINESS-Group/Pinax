/**
 * 
 */
package engine;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import model.StructuredFile;

/**
 * @author pvassil
 *
 */
class SchemaManagerTest {
	static SchemaManager schemaMgr;
	static SparkSession sparkSession;
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		schemaMgr = new SchemaManager();
		sparkSession = SparkSession
				.builder()
				.appName("A simple client to do things with Spark ")
				.config("spark.master", "local")
				.getOrCreate();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterAll
	static void tearDownAfterClass() throws Exception {
		sparkSession.stop();
	}
//
//	/**
//	 * @throws java.lang.Exception
//	 */
//	@BeforeEach
//	void setUp() throws Exception {
//	}
//
//	/**
//	 * @throws java.lang.Exception
//	 */
//	@AfterEach
//	void tearDown() throws Exception {
//	}

	/**
	 * Test method for {@link engine.SchemaManager#registerFileAsDataSource(java.lang.String, java.nio.file.Path, java.lang.String)}.
	 */
	@Test
	void testRegisterFileAsDataSource() {
		long numLines = 0; 
		Path repoPath = Paths.get("src/main/resources/_REPO/RegisteredFiles.tsv");

		int fileListSize = schemaMgr.wipeFileList();
		assertEquals(0,fileListSize);
		
		
		try{
			numLines = schemaMgr.wipeRepoFile();
			assertEquals(1,numLines);
		} catch(IOException e) {
			fail("Could not wipeRepoFile");
			e.printStackTrace();
		}
		
		Path path = Paths.get("src/main/resources/joomlatools__joomla-platform-categories.tsv");
		Path path2 = Paths.get("src/main/resources/try.tsv");
		String fileAlias = schemaMgr.createFileAlias(path.getFileName().toString());
		String fileType = schemaMgr.getFileType(path.getFileName().toString());
		String fileAlias2 = schemaMgr.createFileAlias(path2.getFileName().toString());
		String fileType2 = schemaMgr.getFileType(path2.getFileName().toString());
		try {
			int checkRegistration = schemaMgr.registerFileAsDataSource(fileAlias, path, fileType);
			assertEquals(0,checkRegistration); //check if code 0 returns, meaning all went well
			numLines = Files.lines(repoPath).count();
			assertEquals(2,numLines); 
			assertEquals(1, schemaMgr.getFileList().size()); //check if file list updates
			
			StructuredFile testFile = schemaMgr.getFileList().get(0);
			String alias = testFile.getSfAlias();
			String expectedAlias = "joomlatools__joomla_platform_categories";
			String expectedType = "tsv";
			assertEquals(expectedAlias, alias);
			assertEquals(path, testFile.getSfPath());
			assertEquals(expectedType, testFile.getSfType());
			
			StructuredFile sfTest = schemaMgr.getFileByAliasName(alias);
			assertNotEquals(null,sfTest);
			
			int beforeSize = schemaMgr.getFileList().size();
			List<String[]> beforeRepoContents = schemaMgr.getRepoFileContents();
			schemaMgr.registerFileAsDataSource(fileAlias2, path2, fileType2); //register a second file after getting the contents of the file
			int afterSize = schemaMgr.getFileList().size();
			List<String[]> afterRepoContents = schemaMgr.getRepoFileContents(); //before registering another file to check if get contents works.
			assertNotEquals(beforeSize,afterSize); //if the size changes that means that a structured file was inserted in the file list.
			assertNotEquals(beforeRepoContents,afterRepoContents);
		} catch (IOException e) {
			fail("Registration did not finish properly");
			e.printStackTrace();
		}
		
		try { //this is done after registration to make sure repoFile has files registered
			schemaMgr.wipeFileList();
			int updatedFileListSize = schemaMgr.updateFileList();
			assertNotEquals(0,updatedFileListSize); //check if after the registration the update method works
		} catch (IOException e1) {
			fail("Could not update file list");
			e1.printStackTrace();
		}

	}

}

/**
 * 
 */
package engine;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import model.StructuredFile;

/**
 * @author pvassil
 *
 */
@TestMethodOrder(org.junit.jupiter.api.MethodOrderer.DisplayName.class)
class SchemaManagerTest {
	static SchemaManager schemaMgr;
	static SparkSession sparkSession;
	static Path repoPath = Paths.get("src/main/resources/_REPO/RegisteredFiles.tsv");
	static Path path;
	static Path path2;
	
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
		repoPath = Paths.get("src/main/resources/_REPO/RegisteredFiles.tsv");
		path = Paths.get("src/main/resources/joomlatools__joomla-platform-categories.tsv");
		path2 = Paths.get("src/main/resources/try.tsv");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterAll
	static void tearDownAfterClass() throws Exception {
		sparkSession.stop();
	}

	/**
	 * Test method for {@link engine.SchemaManager#registerFileAsDataSource(java.lang.String, java.nio.file.Path, java.lang.String)}.
	 */
	@Test
	@DisplayName("a")
	void testARegisterFileAsDataSource() {
		long numLines = 0;
		int fileListSize = schemaMgr.wipeFileList();
		assertEquals(0,fileListSize);
		try{
			numLines = schemaMgr.wipeRepoFile();
			assertEquals(1,numLines);
		} catch(IOException e) {
			fail("Could not wipeRepoFile");
			e.printStackTrace();
		}
		String fileAlias = schemaMgr.createFileAlias(path.getFileName().toString());
		String fileType = schemaMgr.getFileType(path.getFileName().toString());
		int checkRegistration = schemaMgr.registerFileAsDataSource(fileAlias, path, fileType);
		assertEquals(0,checkRegistration); //check if code 0 returns, meaning all went well
		assertEquals(1, schemaMgr.getFileList().size()); //check if file list updates
	}
	
	@Test
	@DisplayName("b")
	void testSecondFileRegistration() {
		int beforeSize = schemaMgr.getFileList().size();
		List<String[]> beforeRepoContents;
		String fileAlias2 = schemaMgr.createFileAlias(path2.getFileName().toString());
		String fileType2 = schemaMgr.getFileType(path2.getFileName().toString());
		try {
			beforeRepoContents = schemaMgr.getRepoFileContents();
			schemaMgr.registerFileAsDataSource(fileAlias2, path2, fileType2); //register a second file after getting the contents of the file
			int afterSize = schemaMgr.getFileList().size();
			List<String[]> afterRepoContents = schemaMgr.getRepoFileContents(); //before registering another file to check if get contents works.
			assertNotEquals(beforeSize,afterSize); //if the size changes that means that a structured file was inserted in the file list.
			assertNotEquals(beforeRepoContents,afterRepoContents);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	@Test
	@DisplayName("c")
	void testRegisteredFileInfo() {
		StructuredFile testFile = schemaMgr.getFileList().get(0);
		String alias = testFile.getSfAlias();
		String expectedAlias = "joomlatools__joomla_platform_categories";
		String expectedType = "tsv";
		assertEquals(expectedAlias, alias);
		assertEquals(path, testFile.getSfPath());
		assertEquals(expectedType, testFile.getSfType());
		StructuredFile sfTest = schemaMgr.getFileByAliasName(alias);
		assertNotEquals(null,sfTest);
	}
	
	@Test
	@DisplayName("d")
	void testDelimiterSelector() {
		String expectedCsvDelimiter = ",";
		String expectedTsvDelimiter = "\t";
		String expectedTxtDelimiter = " ";
		
		assertEquals(expectedCsvDelimiter, schemaMgr.delimiterSelector("csv"));
		assertEquals(expectedTsvDelimiter, schemaMgr.delimiterSelector("tsv"));
		assertEquals(expectedTxtDelimiter, schemaMgr.delimiterSelector("txt"));
	}
	
	@Test
	@DisplayName("e")
	void testNullData(){
		try { //this is done after registration to make sure repoFile has files registered
			String fileAlias2 = schemaMgr.createFileAlias(path2.getFileName().toString());
			String fileType2 = schemaMgr.getFileType(path2.getFileName().toString());
			schemaMgr.wipeFileList();
			int updatedFileListSize = schemaMgr.updateFileList();
			assertNotEquals(0,updatedFileListSize); //check if after the registration the update method works
			Path path3 = Paths.get("src/main/resources/nonExistantFile.tsv");
			schemaMgr.registerFileAsDataSource(fileAlias2, path3, fileType2); //trying to register a file that does not exist
			int updatedFileListSize2 = schemaMgr.updateFileList();
			assertNotEquals(0,updatedFileListSize2); //check if the non-existant file was skipped.
		} catch (IOException e1) {
			fail("Could not update file list");
			e1.printStackTrace();
		}
	}
}
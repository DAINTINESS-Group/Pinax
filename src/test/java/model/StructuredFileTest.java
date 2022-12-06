package model;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class StructuredFileTest {

	static FileColumn fc;
	static StructuredFile sf;
	static Path path = Paths.get("this is a dummy path");
	static HashMap<String, FileColumn> expectedColumns;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		List<FileColumn> sfColumns = new ArrayList<FileColumn>();
		fc = new FileColumn("name","string",1);
		sf = new StructuredFile("alias",path,sfColumns,"tsv");
		expectedColumns = new HashMap<String, FileColumn>();
		for(FileColumn fc: sfColumns) {
			expectedColumns.put(fc.getName(), fc);
		}
	}

	@Test
	void testSfFileInfo() {
		String expectedAlias = "alias";
		Path expectedPath = Paths.get("this is a dummy path");
		String expectedType = "tsv";
		
		assertEquals(expectedAlias,sf.getSfAlias());
		assertEquals(expectedPath,sf.getSfPath());
		assertEquals(expectedColumns,sf.getSfColumns());
		assertEquals(expectedType,sf.getSfType());
	}
}
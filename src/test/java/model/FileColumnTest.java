package model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


class FileColumnTest {

	static FileColumn fc;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		fc = new FileColumn("name","string",1);
	}
	
	@Test
	void testFileColumnInfo() {
		String expectedName = "name";
		String expectedType = "string";
		int expectedPosition = 1;
		
		assertEquals(expectedName,fc.getName());
		assertEquals(expectedType,fc.getDataType());
		assertEquals(expectedPosition,fc.getPosition());
	}
}
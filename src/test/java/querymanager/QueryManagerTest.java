package querymanager;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class QueryManagerTest {
	
	static QueryManagerInterface qrMan;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		qrMan = new QueryManagerFactory().createQueryManager();
	}

	@Test
	void testNaiveQueryExpression() throws IOException {
		String expectedNaiveQuery = "SELECT * FROM global_temp.table";
		String testNaive = qrMan.createNaiveQueryExpression("table");
		assertEquals(testNaive,expectedNaiveQuery);
	}
	@Test
	void testProjectionOnlyQueryExpression() {
		String expectedProjectionQuery = "SELECT day FROM global_temp.table";
		List<String> attributes = new ArrayList<String>();
		attributes.add("day");
		String testProjection = qrMan.createProjectionOnlyQueryExpression("table", attributes);
		assertEquals(testProjection,expectedProjectionQuery);
	}
		
	@Test
	void testProjectSelectSingleTableQueryExpression() {
		String expectedSingleQuery = "SELECT day FROM global_temp.table AS t WHERE Month > 3 and Month < 12";
		List<String> singleAttributes = new ArrayList<String>();
		singleAttributes.add("day");
		String filter = "Month > 3 and Month < 12";
		String testSingleQuery = qrMan.createProjectSelectSingleTableQueryExpression("table", "t", singleAttributes, filter); //case sensitive
		assertEquals(testSingleQuery,expectedSingleQuery);
	}
	
	@Test
	void testMultiTableQueryExpression() {
		String expectedMultiTableQuery = "SELECT p.first_name, p.last_name, day, m.first_name, m.last_name FROM global_temp.more_stats AS m LEFT JOIN global_temp.person AS p ON p.ip_address = m.ip_address WHERE p.gender = 'Male'";
		String primaryTable = "more_stats";
		ArrayList<String> joinTables = new ArrayList<String>();
		joinTables.add("person");
		ArrayList<String> tableAliases = new ArrayList<String>();
		tableAliases.add("m");
		tableAliases.add("p");
		ArrayList<String> attributeNames = new ArrayList<String>();
		attributeNames.add("p.first_name");
		attributeNames.add("p.last_name");
		attributeNames.add("day");
		attributeNames.add("m.first_name");
		attributeNames.add("m.last_name");
		ArrayList<String> joinFilters = new ArrayList<String>();
		joinFilters.add("p.ip_address = m.ip_address");
		ArrayList<String> joinTypes = new ArrayList<String>();
		joinTypes.add("LEFT");
		String whereFilter = "p.gender = 'Male'";
		String multiTableQueryExpression = qrMan.createMultiTableQueryExpression(primaryTable,joinTables,tableAliases,attributeNames,joinFilters,joinTypes,whereFilter);
		assertEquals(expectedMultiTableQuery,multiTableQueryExpression);
	}
	
	@Test
	void rainyTestMultiTableQueryExpression() {
		String rainyExpected = " ";
		String primaryTable = "more_stats";
		
		ArrayList<String> joinTables = new ArrayList<String>();
		joinTables.add("person,person2,person4");
		
		ArrayList<String> tableAliases = new ArrayList<String>();
		tableAliases.add("m");
		tableAliases.add("   ");

		ArrayList<String> attributeNames = new ArrayList<String>();
		attributeNames.add("p.first_name");
		attributeNames.add("p.last_name");
		attributeNames.add("day");
		attributeNames.add("m.first_name");
		attributeNames.add("m.last_name");
		
		ArrayList<String> joinFilters = new ArrayList<String>();
		joinFilters.add("p.ip_address = m.ip_address");

		ArrayList<String> joinTypes = new ArrayList<String>();
		joinTypes.add("");
		
		String whereFilter = "p.gender = 'Male'";
		String newexp = qrMan.createMultiTableQueryExpression(primaryTable,joinTables,tableAliases,attributeNames,joinFilters,joinTypes,whereFilter);
		assertEquals(rainyExpected,newexp);
	}
	
}
package querymanager;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class QueryManagerTest {

	@Test
	void test() throws IOException {
		QueryManagerInterface qrMan = new QueryManagerFactory().createQueryManager();
		String expectedNaiveQuery = "SELECT * FROM global_temp.table";
		String testNaive = qrMan.createNaiveQueryExpression("table");
		assertEquals(testNaive,expectedNaiveQuery);
		
		String expectedProjectionQuery = "SELECT day FROM global_temp.table";
		List<String> attributes = new ArrayList<String>();
		attributes.add("day");
		String testProjection = qrMan.createProjectionOnlyQueryExpression("table", attributes);
		assertEquals(testProjection,expectedProjectionQuery);
		
		String expectedSingleQuery = "SELECT day FROM global_temp.table AS t WHERE Month > 3 and Month < 12";
		List<String> singleAttributes = new ArrayList<String>();
		singleAttributes.add("day");
		String filter = "Month > 3 and Month < 12";
		String testSingleQuery = qrMan.createProjectSelectSingleTableQueryExpression("table", "t", singleAttributes, filter); //case sensitive
		assertEquals(testSingleQuery,expectedSingleQuery);
		
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
		
		String rainyExpected = " ";
		primaryTable = "more_stats";
		
		joinTables.clear();
		tableAliases.clear();
		attributeNames.clear();
		joinTypes.clear();
		
		joinTables.add("person,person2,person4");
		
		tableAliases.add("m");
		tableAliases.add("   ");

		attributeNames.add("p.first_name");
		attributeNames.add("p.last_name");
		attributeNames.add("day");
		attributeNames.add("m.first_name");
		attributeNames.add("m.last_name");
		
		joinFilters.add("p.ip_address = m.ip_address");

		joinTypes.add("");
		whereFilter = "p.gender = 'Male'";
		String newexp = qrMan.createMultiTableQueryExpression(primaryTable,joinTables,tableAliases,attributeNames,joinFilters,joinTypes,whereFilter);
		assertEquals(rainyExpected,newexp);

	}

}

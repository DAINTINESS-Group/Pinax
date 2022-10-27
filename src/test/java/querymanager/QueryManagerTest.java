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

	}

}

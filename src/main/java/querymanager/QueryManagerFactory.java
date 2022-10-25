package querymanager;

import java.io.IOException;



public class QueryManagerFactory {

	public QueryManagerInterface createQueryManager() throws IOException {
		return new QueryManager();
	}
}

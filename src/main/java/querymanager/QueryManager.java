package querymanager;

import java.util.List;

//MUST DECIDE WHAT THE QMgr DOES

public class QueryManager implements QueryManagerInterface{
	
	//MUST MAKE TESTS TO TEST ME
	public String createNaiveQueryExpression(String table) {
		return "SELECT * FROM global_temp." + table;
	}
	
	//MUST MAKE TESTS TO TEST ME
	public String createProjectionOnlyQueryExpression(String table, List<String> attributeNames) {
		return "SELECT " + String.join(", ", attributeNames) + " FROM global_temp." + table;
	}
	
	//MUST MAKE TESTS TO TEST ME
	public String createProjectSelectSingleTableQueryExpression(String table, String tableAlias, List<String> attributeNames, String filters) {
		return "SELECT " + String.join(", ", attributeNames)
			+ " FROM global_temp." + table + " AS " + tableAlias
			+ " WHERE " + filters;
	}
}
package querymanager;

import java.util.List;

//MUST DECIDE WHAT THE QMgr DOES

public class QueryManager implements QueryManagerInterface{
	
	public String createNaiveQueryExpression(String table) {
		return "SELECT * FROM global_temp." + table;
	}
	
	public String createProjectionOnlyQueryExpression(String table, List<String> attributeNames) {
		return "SELECT " + String.join(", ", attributeNames) + " FROM global_temp." + table;
	}
	
	public String createProjectSelectSingleTableQueryExpression(String table, String tableAlias, List<String> attributeNames, String filters) {
		return "SELECT " + String.join(", ", attributeNames)
			+ " FROM global_temp." + table + " AS " + tableAlias
			+ " WHERE " + filters;
	}
	
	public String createMultiTableQueryExpression(
			String primaryTable,
			List<String> joinTables,
			List<String> tableAliases,
			List<String> attributeNames,
			List<String> joinFilters,
			List<String> joinTypes,
			String whereFilter) {
		String query = "SELECT " + String.join(", ", attributeNames)
				+ " FROM global_temp." + primaryTable + " AS " + tableAliases.get(0);
		tableAliases.remove(0);
		for(int i=0;i<joinTypes.size();i++) {
			query += " " + joinTypes.get(i) + " JOIN global_temp." + joinTables.get(i) + " AS " + tableAliases.get(i) + " ON " + joinFilters.get(i);
		}
		if(whereFilter.length() > 1) {
			query += " WHERE " + whereFilter;
		}
		return query;
	}
}
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
		
		joinTables = trimmerMethod(joinTables);
		joinTypes = trimmerMethod(joinTypes);
		tableAliases = trimmerMethod(tableAliases);
		
		if(joinTables.size() != tableAliases.size() || joinTypes.size() != joinTables.size()) {
			System.out.println("The number of arguments is wrong");
			query = " ";
			return query;
		}
		for(int i=0; i<joinTypes.size(); i++) {
			query += " " + joinTypes.get(i) + " JOIN global_temp." + joinTables.get(i) + " AS " + tableAliases.get(i);
		}
		for(int i=0; i<joinFilters.size(); i++) {
			if(joinFilters.get(i) != null && !joinFilters.get(i).trim().isEmpty()) {
				query += " ON " + joinFilters.get(i);
			}
		}
		if(whereFilter != null && !whereFilter.trim().isEmpty()) {
			query += " WHERE " + whereFilter;
		}
		return query;
	}
	
	private List<String> trimmerMethod(List<String> list){
		for(int i=0; i<list.size(); i++) {
			if(list.get(i) != null && !list.get(i).trim().isEmpty()) {
				continue;
			}
			else {
				list.remove(i);			
			}
		}
		return list;
	}
}
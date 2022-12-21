package querymanager;

import java.util.List;

public interface QueryManagerInterface {
	
	public String createNaiveQueryExpression(String table);
	public String createProjectionOnlyQueryExpression(String table, List<String> attributeNames);
	public String createProjectSelectSingleTableQueryExpression(String table, String tableAlias, List<String> attributeNames, String filters);
	public String createMultiTableQueryExpression(String primaryTable, List<String> joinTables,
			List<String> tableAliases, List<String> attributeNames, List<String> joinFilters, List<String> joinTypes,
			String whereFilter);
}
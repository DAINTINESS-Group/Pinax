package querymanager;

import java.util.ArrayList;
import java.util.List;

public interface QueryManagerInterface {
	
	public String createNaiveQueryExpression(String table);
	public String createProjectionOnlyQueryExpression(String table, List<String> attributeNames);
	public String createProjectSelectSingleTableQueryExpression(String table, String tableAlias, List<String> attributeNames, String filters);
	public String createMultiTableQueryExpression(String primaryTable, ArrayList<String> joinTables,
			ArrayList<String> tableAliases, ArrayList<String> attributeNames, ArrayList<String> joinFilters, ArrayList<String> joinTypes,
			String whereFilter);
}
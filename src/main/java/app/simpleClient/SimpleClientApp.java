package app.simpleClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import querymanager.QueryManagerFactory;
import querymanager.QueryManagerInterface;

public class SimpleClientApp {
	
	private UserInterface mainw = UserInterface.getSingletonView();
	
	public void runCustomQuery(String customQuery) {
		customQuery = customQuery.replace("\n", " ").replace("\r", " ");
		if(checkTFFilled(customQuery)) {
			mainw.getSparkSession().sql(customQuery).show(99,false);
		}
	}
	
	public void queryRunner() throws IOException {
		QueryManagerInterface qrMan = new QueryManagerFactory().createQueryManager();
		Dataset<Row> results = null;
		if(pickProperQueryConstructor() == 1) {
			String primaryTableString = mainw.getPrimaryTableTextField().getText();
			String naiveQueryExpression = qrMan.createNaiveQueryExpression(primaryTableString);
			//mainw.getSparkSession().sql(naiveQueryExpression).show(99,false);
			results = mainw.getSparkSession().sql(naiveQueryExpression);
		}
		else if(pickProperQueryConstructor() == 2) {
			String primaryTableString = mainw.getPrimaryTableTextField().getText();
			String attributesString = mainw.getAttributeNamesTextField().getText();
			String projectionOnlyQueryExpression = qrMan.createProjectionOnlyQueryExpression(primaryTableString,createTFList(attributesString));
			//mainw.getSparkSession().sql(projectionOnlyQueryExpression).show(99,false);
			results = mainw.getSparkSession().sql(projectionOnlyQueryExpression);
		}
		else if(pickProperQueryConstructor() == 3) {
			String primaryTableString = mainw.getPrimaryTableTextField().getText();
			String attributesString = mainw.getAttributeNamesTextField().getText();
			String tableAliasString = mainw.getTableAliasesTextField().getText();
			String whereFilterString = mainw.getWhereFilterTextField().getText();
			String projectSelectSingleTableQueryExpression = 
					qrMan.createProjectSelectSingleTableQueryExpression(
							primaryTableString,
							tableAliasString,
							createTFList(attributesString),
							whereFilterString
							);
			//mainw.getSparkSession().sql(projectSelectSingleTableQueryExpression).show(99,false);
			results = mainw.getSparkSession().sql(projectSelectSingleTableQueryExpression);
		}
		else if(pickProperQueryConstructor() == 4) {
			String primaryTableString = mainw.getPrimaryTableTextField().getText();
			String attributesString = mainw.getAttributeNamesTextField().getText();
			String tableAliasString = mainw.getTableAliasesTextField().getText();
			String whereFilterString = mainw.getWhereFilterTextField().getText();
			String joinFilterString = mainw.getJoinFiltersTextField().getText();
			String joinTablesString = mainw.getJoinTablesTextField().getText();
			String joinTypesString = mainw.getJoinTypesTextField().getText();
			String multiTableQueryExpression = 
					qrMan.createMultiTableQueryExpression(
							primaryTableString,
							createTFList(joinTablesString),
							createTFList(tableAliasString),
							createTFList(attributesString),
							createTFList(joinFilterString),
							createTFList(joinTypesString),
							whereFilterString
							);
			//mainw.getSparkSession().sql(multiTableQueryExpression).show(99,false);
			results = mainw.getSparkSession().sql(multiTableQueryExpression);
		}
		ResultFrame frame = new ResultFrame();
		frame.setVisible(true);
		frame.createResultJTable(results);
	}
	
	public int pickProperQueryConstructor() {
		if(checkTFFilled(mainw.getPrimaryTableTextField().getText())) {
			if(!checkTFFilled(mainw.getAttributeNamesTextField().getText())) {
				return 1;
			}
			else if(checkTFFilled(mainw.getJoinTypesTextField().getText()) 
					&& checkTFFilled(mainw.getTableAliasesTextField().getText())
					&& checkTFFilled(mainw.getJoinTablesTextField().getText())) {
				return 4;
			}
			else if(checkTFFilled(mainw.getTableAliasesTextField().getText()) && checkTFFilled(mainw.getWhereFilterTextField().getText())) {
				return 3;
			}
			else{
				return 2;
			}
		} 
		return -1;
	}
	
	public boolean checkTFFilled(String str) {
		if(str != null && !str.trim().isEmpty()) {
			return true;
		}
		return false;
	}
	
	public List<String> createTFList(String contents){
		List<String> items = Arrays.asList(contents.split(",")).stream().filter(str -> !str.isEmpty()).collect(Collectors.toList());
		return items;
	}
}
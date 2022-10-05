package querymanager;

import java.util.Scanner;

public class QueryManager {
	
	
	public String createQueryExpression() {
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		scanner.useDelimiter(System.getProperty("line.separator"));
		int queryCreator = 1;
		System.out.println("Type one of the above names to choose the table you want to get data from");
		String sfAlias = scanner.next();
		String queryType = scanner.next();
		//String columnName = scanner.next();
		String queryExpression = queryType + " from global_temp."+ sfAlias;
		//queryExpression += scanner.next();
		System.out.println("Do you want to add more parameters to the query? If so type 0 ");
		Integer flag = scanner.nextInt();
		if(flag == 0) {
			queryExpression += " where ";
			queryExpression += createSQLParameters(scanner);
			System.out.println("If you want to add another parameter type 0, else type any int to stop");
			queryCreator = scanner.nextInt();
			while(queryCreator == 0) {
				System.out.println("Type the logical operator you want to use");
				queryExpression += scanner.next() + " ";
				queryExpression += createSQLParameters(scanner);
				System.out.println("If you want to add another parameter type 0, else type any int to stop");
				queryCreator = scanner.nextInt();
			}
			System.out.println("If you want to groupby or orderby or something type 0, else type any int");
			Integer flag2 = scanner.nextInt();
			if(flag2 == 0) {
				System.out.println("Add Statement");
				queryExpression += " " + scanner.next() + " ";
				System.out.println("Chose column to do the statement for");
				queryExpression += scanner.next();
			}
		}
		return queryExpression;
	}
	
	public String createSQLParameters(Scanner scanner) {
		String queryExpression = "";
		System.out.println("Give column");
		String columnName = scanner.next();
		queryExpression += columnName;
		System.out.println("Give operator");
		String operator = scanner.next();
		queryExpression += " " + operator;
		System.out.println("Give value. In case of String include '' ");
		String value = scanner.next();
		queryExpression += " " + value + " ";
		
		return queryExpression;
	}
}

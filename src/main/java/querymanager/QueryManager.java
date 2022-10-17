package querymanager;

import java.util.Scanner;

public class QueryManager {
	
	
	public String createQueryExpression() {
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		scanner.useDelimiter(System.getProperty("line.separator"));
		String queryExpression = "";
		try {
			System.out.println("Type one of the above names to choose the table you want to get data from");
			String sfAlias = scanner.next();
			System.out.println("Type what you would like the query to do");
			String queryType = scanner.next();
			queryExpression += queryType + " from global_temp."+ sfAlias;
			System.out.println("Type an acronym for the table. Use it in case the files have common column names");
			String acronym = scanner.next();
			queryExpression += " " + "as " + acronym;
			System.out.println("If you would like to join another table type 0");
			Integer joiner = scanner.nextInt();
			while(joiner == 0) {
				queryExpression += createSqlJoinedTable(scanner);
				System.out.println("If you want to join another table type 0");
				joiner = scanner.nextInt();
			}
			System.out.println("Do you want to add more parameters to the query? If so type 0 ");
			Integer paramFlag = scanner.nextInt();
			if(paramFlag == 0) {
				queryExpression += " where ";
				while(paramFlag == 0) {
					System.out.println("Type the logical operator you want to use (and,or,...)");
					queryExpression += scanner.next() + " ";
					queryExpression += createSQLParameters(scanner);
					System.out.println("If you want to add another parameter type 0, else type any int to stop");
					paramFlag = scanner.nextInt();
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
		}
		catch (Exception e) {
			System.out.println("You did not input the proper data type! Please reconstruct the query");
			scanner.next();
		}
		return queryExpression;
	}
	
	public String createSqlJoinedTable(Scanner scanner) {
		String expression = " ";
		System.out.println("Type the name of the table you would like to join");
		String alias = scanner.next();
		System.out.println("Type the kind of join you would like to perform");
		String typeOfJoin = scanner.next();
		System.out.println("Type an acronym for the table");
		String acronym = scanner.next();
		expression += typeOfJoin + " JOIN global_temp." + alias + " as " + acronym + " ON " + createSQLParameters(scanner);
		return expression;
	}
	public String createSQLParameters(Scanner scanner) {
		System.out.println("Type the expression you want to add. In case you want to compare with a string include it in '' ");
		String queryExpression = scanner.next();
		return queryExpression;
	}
}

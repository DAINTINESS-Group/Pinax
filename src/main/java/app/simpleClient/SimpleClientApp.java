package app.simpleClient;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.LookAndFeel;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.JTable;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.opencsv.exceptions.CsvException;

import engine.SchemaManagerInterface;
import engine.SchemaManagerFactory;
import model.StructuredFile;
import scala.Tuple2;
import engine.FunctionManager;


public class SimpleClientApp {
	
	private static SimpleClientApp mainw;
	private JFrame frame;
	private FunctionManager func = new FunctionManager();
	private JPanel leftPanel;
	private JPanel rightPanel;
	private SparkSession spark = SparkSession
			.builder()
			.appName("A simple client to do things with Spark ")
			.config("spark.master", "local")
			.getOrCreate();
		
		
	// WE DONOT WANT A MAIN WITH INTERACTION. WILL HAVE A GUI FOR THIS
	//FOR THE MOMENT WE NEED A SIMPLE CLIENT THAT MAKES BACK-END CALLS VIA THE INTERFACES
	//KIND-LIKE-A TEST
	public static void main(String[] args) throws AnalysisException, IOException, CsvException {
		Logger rootLogger = Logger.getRootLogger(); //remove the messages of spark
		rootLogger.setLevel(Level.ERROR);
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					mainw = new SimpleClientApp();
					mainw.initialize();
					mainw.frame.setVisible(true);
					mainw.loadFiles();
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		@SuppressWarnings("unused")
		LookAndFeel old = UIManager.getLookAndFeel();
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		}
		catch (Throwable ex) {
			old = null;
		}
		
		/*SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		QueryManagerInterface qrMan = new QueryManagerFactory().createQueryManager();
	
		Dataset<Row> df = null;
		
		schMan.wipeRepoFile();
		schMan.wipeFileList();
		
		String[] filePaths = {"src/main/resources/more_stats.tsv",
								"src/main/resources/try.tsv", 
								"src/main/resources/person.tsv", 
								"src/main/resources/joomlatools__joomla-platform-categories.tsv"};
		for(String s: filePaths) {
			Path path = Paths.get(s);
			String fileAlias = schMan.createFileAlias(path.getFileName().toString());
			String fileType = schMan.getFileType(path.getFileName().toString());
			schMan.registerFileAsDataSource(fileAlias, path, fileType);
		}
		
		List<StructuredFile> fileList = schMan.getFileList();
		System.out.println("------------------------------------");
		for(StructuredFile sf: fileList) {
			df = spark.read().option("delimiter", schMan.delimiterSelector(sf.getSfType())).option("header", "true").option("inferSchema","true").csv(sf.getSfPath().toRealPath().toString());
			df.createGlobalTempView(sf.getSfAlias());
			System.out.println(sf.getSfAlias());
		}
		System.out.println("------------------------------------");
	
		String naiveQueryExpression = qrMan.createNaiveQueryExpression("more_stats");
		spark.sql(naiveQueryExpression).show((int)df.count(),false);
		
		List<String> listOfAttributes = new ArrayList<String>();
		listOfAttributes.add("first_name");
		listOfAttributes.add("last_name");
		listOfAttributes.add("day");
		String projectionOnlyQueryExpression = qrMan.createProjectionOnlyQueryExpression("more_stats", listOfAttributes);
		spark.sql(projectionOnlyQueryExpression).show((int)df.count(),false);
		
		List<String> anotherListOfAttributes = new ArrayList<String>();
		anotherListOfAttributes.add("first_name");
		anotherListOfAttributes.add("last_name");
		anotherListOfAttributes.add("day");
		String filters = " stats.last_name = 'kon' and first_name = 'stelios'";
		String projectSelectSingleTableQueryExpression = qrMan.createProjectSelectSingleTableQueryExpression("more_stats", "stats", anotherListOfAttributes, filters);
		System.out.println(projectSelectSingleTableQueryExpression);
		spark.sql(projectSelectSingleTableQueryExpression).show((int)df.count(),false);
		
		
		String primaryTable = "more_stats";
		ArrayList<String> joinTables = new ArrayList<String>();
		joinTables.add("person");
		joinTables.add("try");
		ArrayList<String> tableAliases = new ArrayList<String>();
		tableAliases.add("m");
		tableAliases.add("p");
		tableAliases.add("t");
		ArrayList<String> attributeNames = new ArrayList<String>();
		attributeNames.add("p.first_name");
		attributeNames.add("p.last_name");
		attributeNames.add("day");
		attributeNames.add("m.first_name");
		attributeNames.add("m.last_name");
		attributeNames.add("t.PrjActivity");
		ArrayList<String> joinFilters = new ArrayList<String>();
		joinFilters.add("p.ip_address = m.ip_address");
		joinFilters.add(" ");
		ArrayList<String> joinTypes = new ArrayList<String>();
		joinTypes.add("LEFT");
		joinTypes.add("LEFT");
		String whereFilter = "p.gender = 'Male' and t.Month > 35";
		
		String multiTableQueryExpression = qrMan.createMultiTableQueryExpression(primaryTable,joinTables,tableAliases,attributeNames,joinFilters,joinTypes,whereFilter);
		System.out.println(multiTableQueryExpression);
		spark.sql(multiTableQueryExpression).show((int)df.count(),false);
		
		//Dataset<Row> df = spark.read().option("delimiter", "\t").option("header", "true").option("inferSchema","true").csv(path.toRealPath().toString());
		//df = spark.read().option("delimiter", ",").option("header", "true").option("inferSchema","true").csv();
		//ignore any front-end mambo jumbo. do it programmatically in the client.
		//try some stuff to see if it works
		spark.stop();*/
		System.out.println("You exited the program");
	}
	
	public static SimpleClientApp getSingletonView()
	{
		if(mainw == null)
			mainw = new SimpleClientApp();
		return mainw;
	}
	
	public JFrame getFrame() {
		return frame;
	}
	
	public void closeOperation() {
		spark.stop();
		
	}
	
	public JPanel getLeftPanel() {
		return leftPanel;
	}
	
	public JPanel getRightPanel() {
		return rightPanel;
	}
	
	/**
	 * Initialize the contents of the frame.
	 * @throws IOException 
	 */
	private void initialize() throws IOException {
		//QueryManagerInterface qrMan = new QueryManagerFactory().createQueryManager();
		SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		frame = new JFrame();
		frame.setTitle("MSAccess");
		frame.setBounds(50, 25, 1200, 750);
		frame.addWindowListener(new WindowAdapter() {
	        public void windowClosing(WindowEvent e) {
	        	int confirmed = JOptionPane.showConfirmDialog(null,"Are you sure you want to exit the program?", "Exit Program Message Box",JOptionPane.YES_NO_OPTION);
	        	if (confirmed == JOptionPane.YES_OPTION) {
	        		try {
	        			schMan.wipeRepoFile();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
	        		spark.stop();
	        		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
	        	}
	        	else {
	        		frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
	        	}
	        }
	    });
		frame.getContentPane().setLayout(new BorderLayout(0, 0));
		
		JMenuBar menuBar = new JMenuBar();
		frame.setJMenuBar(menuBar);
		
		JButton openItem = new JButton("Add Files");
		openItem.addActionListener(func.createCommand("Select"));
		menuBar.add(openItem);
		
		JSplitPane splitPane = new JSplitPane();
		frame.getContentPane().add(splitPane, BorderLayout.CENTER);
		splitPane.setDividerLocation(300);
		
		leftPanel = new JPanel();
		leftPanel.setLayout(new BoxLayout(leftPanel, BoxLayout.Y_AXIS));

		JScrollPane scrollPane = new JScrollPane();
		splitPane.setLeftComponent(scrollPane);
		scrollPane.setViewportView(leftPanel);
		scrollPane.setPreferredSize(new Dimension(60,80));
    
		rightPanel = new JPanel();
		splitPane.setRightComponent(rightPanel);
		rightPanel.setLayout(null);	
	}
	
	public void createJTables(String fileAlias, Dataset<Row> df) {
		ArrayList<String[]> dataOfFile = new ArrayList<String[]>();
		String nameOfTable[] = {fileAlias,"Type"};
		for(Tuple2<String, String> element : df.dtypes()) {
			String[] s = {element._1,element._2.substring(0,element._2.length()-4)};
			dataOfFile.add(s);
		}
		String tableData[][] = new String[dataOfFile.size()][];
		for(int i=0;i<dataOfFile.size();i++) {
			tableData[i] = dataOfFile.get(i);
		}
		JPanel panel = new JPanel();
		JTable jt=new JTable(tableData,nameOfTable);
		Dimension d = jt.getPreferredSize();
		jt.setPreferredScrollableViewportSize(d);
		jt.setDefaultEditor(Object.class, null);
		panel.add(jt);
		panel.validate();
		JScrollPane sp=new JScrollPane(jt);    
		panel.add(sp);
		mainw.getLeftPanel().add(panel);
		SwingUtilities.updateComponentTreeUI(mainw.getFrame());
		/*Component[] components = mainw.getLeftPanel().getComponents();

        for (Component component : components) {
            System.out.println(component);
        }*/
	}
	
	public void loadFiles() throws AnalysisException, IOException {
		SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		Dataset<Row> df = null;
		List<StructuredFile> fileList = schMan.getFileList();
		for(StructuredFile sf: fileList) {
			try {
				df = spark.read().option("delimiter", schMan.delimiterSelector(sf.getSfType())).option("header", "true").option("inferSchema","true").csv(sf.getSfPath().toRealPath().toString());
				df.createGlobalTempView(sf.getSfAlias());
				createJTables(sf.getSfAlias(),df);
				System.out.println(sf.getSfAlias());
			} catch (AnalysisException e) {
				System.out.println("df exists already");
			}
		}

	}
}
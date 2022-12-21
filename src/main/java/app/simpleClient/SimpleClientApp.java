package app.simpleClient;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
import querymanager.QueryManagerFactory;
import querymanager.QueryManagerInterface;
import scala.Tuple2;
import engine.FunctionManager;

import javax.swing.JLabel;
import javax.swing.JTextField;


public class SimpleClientApp {
	
	private static SimpleClientApp mainw;
	private Dataset<Row> df;
	private JFrame frame;
	private FunctionManager function = new FunctionManager();
	private JPanel leftPanel;
	private JPanel rightPanel;
	private SparkSession spark = SparkSession
			.builder()
			.appName("A simple client to do things with Spark ")
			.config("spark.master", "local")
			.getOrCreate();
	private JTextField primaryTableTextField;
	private JTextField joinTablesTextField;
	private JTextField tableAliasesTextField;
	private JTextField attributeNamesTextField;
	private JTextField joinFiltersTextField;
	private JTextField joinTypesTextField;
	private JTextField whereFilterTextField;

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
	
	public SparkSession getSparkSession() {
		return spark;
	}
	
	public JPanel getLeftPanel() {
		return leftPanel;
	}
	
	public JPanel getRightPanel() {
		return rightPanel;
	}
	
	public JTextField getPrimaryTableTextField() {
		return primaryTableTextField;
	}

	public void setPrimaryTableTextField(JTextField primaryTableTextField) {
		this.primaryTableTextField = primaryTableTextField;
	}
	
	public JTextField getJoinTablesTextField() {
		return joinTablesTextField;
	}
	
	public JTextField getTableAliasesTextField() {
		return tableAliasesTextField;
	}
	
	public JTextField getAttributeNamesTextField() {
		return attributeNamesTextField;
	}
	
	public JTextField getJoinFiltersTextField() {
		return joinFiltersTextField;
	}
	
	/**
	 * Initialize the contents of the frame.
	 * @throws IOException 
	 */
	private void initialize() throws IOException {
		SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		frame = new JFrame();
		frame.setTitle("Pinax");
		frame.setBounds(50, 25, 575, 500);
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
		openItem.addActionListener(function.createCommand("Select"));
		menuBar.add(openItem);
		
		JSplitPane splitPane = new JSplitPane();
		frame.getContentPane().add(splitPane, BorderLayout.CENTER);
		splitPane.setDividerLocation(190);
		
		leftPanel = new JPanel();
		leftPanel.setLayout(new BoxLayout(leftPanel, BoxLayout.Y_AXIS));

		JScrollPane scrollPane = new JScrollPane();
		splitPane.setLeftComponent(scrollPane);
		scrollPane.setViewportView(leftPanel);
		scrollPane.setPreferredSize(new Dimension(60,80));
    
		rightPanel = new JPanel();
		splitPane.setRightComponent(rightPanel);
		rightPanel.setLayout(null);
		
		JLabel lblNewLabel = new JLabel("Primary Table");
		lblNewLabel.setBounds(10, 11, 70, 14);
		rightPanel.add(lblNewLabel);
		
		primaryTableTextField = new JTextField();
		primaryTableTextField.setBounds(10, 36, 313, 20);
		rightPanel.add(primaryTableTextField);
		primaryTableTextField.setColumns(10);
		
		JLabel joinTablesLabel = new JLabel("Tables for joining");
		joinTablesLabel.setBounds(10, 67, 91, 14);
		rightPanel.add(joinTablesLabel);
		
		joinTablesTextField = new JTextField();
		joinTablesTextField.setBounds(10, 92, 313, 20);
		rightPanel.add(joinTablesTextField);
		joinTablesTextField.setColumns(10);
		
		JLabel tableAliasesLabel = new JLabel("Table Aliases");
		tableAliasesLabel.setBounds(10, 123, 70, 14);
		rightPanel.add(tableAliasesLabel);
		
		tableAliasesTextField = new JTextField();
		tableAliasesTextField.setBounds(10, 148, 313, 20);
		rightPanel.add(tableAliasesTextField);
		tableAliasesTextField.setColumns(10);
		
		JLabel attributeNamesLabel = new JLabel("Attributes to be returned");
		attributeNamesLabel.setBounds(10, 179, 132, 14);
		rightPanel.add(attributeNamesLabel);
		
		attributeNamesTextField = new JTextField();
		attributeNamesTextField.setBounds(10, 204, 313, 20);
		rightPanel.add(attributeNamesTextField);
		attributeNamesTextField.setColumns(10);
		
		JLabel joinFiltersLabel = new JLabel("Join Filters");
		joinFiltersLabel.setBounds(10, 235, 70, 14);
		rightPanel.add(joinFiltersLabel);
		
		joinFiltersTextField = new JTextField();
		joinFiltersTextField.setBounds(10, 260, 313, 20);
		rightPanel.add(joinFiltersTextField);
		joinFiltersTextField.setColumns(10);
		
		JLabel joinTypesLabel = new JLabel("Join Types");
		joinTypesLabel.setBounds(10, 291, 70, 14);
		rightPanel.add(joinTypesLabel);
		
		joinTypesTextField = new JTextField();
		joinTypesTextField.setBounds(10, 316, 313, 20);
		rightPanel.add(joinTypesTextField);
		joinTypesTextField.setColumns(10);
		
		JLabel whereFilterLabel = new JLabel("Where Filters");
		whereFilterLabel.setBounds(10, 347, 70, 14);
		rightPanel.add(whereFilterLabel);
		
		whereFilterTextField = new JTextField();
		whereFilterTextField.setBounds(10, 372, 313, 20);
		rightPanel.add(whereFilterTextField);
		whereFilterTextField.setColumns(10);
		
		JButton btnNewButton = new JButton("Run Query");
		btnNewButton.addActionListener(new ActionListener() { 
		    public void actionPerformed(ActionEvent e) {
		    	try {
					queryRunner();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    }
		});
		btnNewButton.setBounds(200, 400, 123, 23);
		rightPanel.add(btnNewButton);
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
		df = null;
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
	
	public void queryRunner() throws IOException {
		QueryManagerInterface qrMan = new QueryManagerFactory().createQueryManager();
		if(pickProperQueryConstructor() == 1) {
			String naiveQueryExpression = qrMan.createNaiveQueryExpression(primaryTableTextField.getText());
			spark.sql(naiveQueryExpression).show((int)df.count(),false);
		}
		else if(pickProperQueryConstructor() == 2) {
			String projectionOnlyQueryExpression = qrMan.createProjectionOnlyQueryExpression(primaryTableTextField.getText(),createTFList(attributeNamesTextField.getText()));
			spark.sql(projectionOnlyQueryExpression).show((int)df.count(),false);
		}
		else if(pickProperQueryConstructor() == 3) {
			String projectSelectSingleTableQueryExpression = 
					qrMan.createProjectSelectSingleTableQueryExpression(
							primaryTableTextField.getText(),
							tableAliasesTextField.getText(),
							createTFList(attributeNamesTextField.getText()),
							whereFilterTextField.getText()
							);
			spark.sql(projectSelectSingleTableQueryExpression).show((int)df.count(),false);
		}
		else if(pickProperQueryConstructor() == 4) {
			String multiTableQueryExpression = 
					qrMan.createMultiTableQueryExpression(
							primaryTableTextField.getText(),
							createTFList(joinTablesTextField.getText()),
							createTFList(tableAliasesTextField.getText()),
							createTFList(attributeNamesTextField.getText()),
							createTFList(joinFiltersTextField.getText()),
							createTFList(joinTypesTextField.getText()),
							whereFilterTextField.getText()
							);
			spark.sql(multiTableQueryExpression).show((int)df.count(),false);
		}
	}
	
	public int pickProperQueryConstructor() {
		if(checkTFFilled(primaryTableTextField.getText())) {
			if(!checkTFFilled(attributeNamesTextField.getText())) {
				return 1;
			}
			else if(checkTFFilled(joinTypesTextField.getText()) 
					&& checkTFFilled(tableAliasesTextField.getText())
					&& checkTFFilled(joinTablesTextField.getText())) {
				return 4;
			}
			else if(checkTFFilled(tableAliasesTextField.getText()) && checkTFFilled(whereFilterTextField.getText())) {
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
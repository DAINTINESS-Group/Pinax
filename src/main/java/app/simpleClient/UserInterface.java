package app.simpleClient;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.LookAndFeel;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.opencsv.exceptions.CsvException;

//import engine.FunctionManager;
import engine.SchemaManagerFactory;
import engine.SchemaManagerInterface;
import model.StructuredFile;
import scala.Tuple2;

public class UserInterface {
	private static UserInterface mainw;
	private Dataset<Row> df;
	private JFrame frame;
	private JPanel leftPanel;
	private JPanel rightPanel;
	private JButton queryRunner;
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

	public static void main(String[] args) throws AnalysisException, IOException, CsvException {
		Logger rootLogger = Logger.getRootLogger(); //remove the messages of spark
		rootLogger.setLevel(Level.ERROR);
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					mainw = new UserInterface();
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
	
	public static UserInterface getSingletonView()
	{
		if(mainw == null)
			mainw = new UserInterface();
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
	
	public JTextField getWhereFilterTextField() {
		return whereFilterTextField;
	}
	
	public JTextField getJoinTypesTextField() {
		return joinTypesTextField;
	}
	
	/**
	 * Initialize the contents of the frame.
	 * @throws IOException 
	 */
	private void initialize() throws IOException {
		SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		frame = new JFrame();
		frame.setTitle("Pinax");
		frame.setBounds(50, 25, 550, 500);
		frame.addWindowListener(new WindowAdapter() {
	        public void windowClosing(WindowEvent e) {
	        	int confirmed = JOptionPane.showConfirmDialog(null,"Are you sure you want to exit the program?", "Exit Program Message Box",JOptionPane.YES_NO_OPTION);
	        	if (confirmed == JOptionPane.YES_OPTION) {
	        		try {
	        			schMan.wipeRepoFile();
	        			schMan.wipeFileList();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
	        		spark.stop();
	        		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
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
		openItem.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
		    	long start = System.currentTimeMillis();
		    	FileSelector selector = new FileSelector();
				selector.actionPerformed(e);
				long end = System.currentTimeMillis();
				float sec = (end - start) / 1000F; 
				System.out.println(sec + " seconds");
		    }
		});
		menuBar.add(openItem);
		
		JButton helpItem = new JButton("Help");
		helpItem.addActionListener(new ActionListener() { 
		    public void actionPerformed(ActionEvent e) {
		    	if (Desktop.isDesktopSupported()) {
		    		try {
		    			File myFile = new File("src/main/resources/Instruction_Manual/PinaxInstructionManual.pdf");
		    			Desktop.getDesktop().open(myFile);
		    		} catch (IOException ex) {
		    			System.out.println("no application registered for PDFs");
		    		}
		    	}
		    	else {
		    		HelpFrame helper = new HelpFrame();
			    	helper.setVisible(true);
		    	}
		    }
		});
		menuBar.add(helpItem);
		
		JButton wipeItem = new JButton("Wipe Data");
		wipeItem.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
		    	int confirmed = JOptionPane.showConfirmDialog(null,"Are you sure you want to wipe all the registered data", "Wipe Data Message",JOptionPane.YES_NO_OPTION);
	        	if (confirmed == JOptionPane.YES_OPTION) {
	        		try {
						wipeMethod();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
	        	}
		    }
		});
		menuBar.add(wipeItem);
		
		
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
		primaryTableTextField.setBounds(10, 36, 318, 20);
		primaryTableTextField.setToolTipText("eg: person ");
		rightPanel.add(primaryTableTextField);
		primaryTableTextField.setColumns(10);
		
		JLabel joinTablesLabel = new JLabel("Tables for joining");
		joinTablesLabel.setBounds(10, 67, 91, 14);
		rightPanel.add(joinTablesLabel);
		
		joinTablesTextField = new JTextField();
		joinTablesTextField.setBounds(10, 92, 318, 20);
		joinTablesTextField.setToolTipText("eg: try,more_stats ");
		rightPanel.add(joinTablesTextField);
		joinTablesTextField.setColumns(10);
		
		JLabel tableAliasesLabel = new JLabel("Table Aliases");
		tableAliasesLabel.setBounds(10, 123, 70, 14);
		rightPanel.add(tableAliasesLabel);
		
		tableAliasesTextField = new JTextField();
		tableAliasesTextField.setBounds(10, 148, 318, 20);
		tableAliasesTextField.setToolTipText("eg: p,m,c ");
		rightPanel.add(tableAliasesTextField);
		tableAliasesTextField.setColumns(10);
		
		JLabel attributeNamesLabel = new JLabel("Attributes to be returned");
		attributeNamesLabel.setBounds(10, 179, 132, 14);
		rightPanel.add(attributeNamesLabel);
		
		attributeNamesTextField = new JTextField();
		attributeNamesTextField.setBounds(10, 204, 318, 20);
		attributeNamesTextField.setToolTipText("eg: month,p.id,m.first_name ");
		rightPanel.add(attributeNamesTextField);
		attributeNamesTextField.setColumns(10);
		
		JLabel joinFiltersLabel = new JLabel("Join Filters");
		joinFiltersLabel.setBounds(10, 235, 70, 14);
		rightPanel.add(joinFiltersLabel);
		
		joinFiltersTextField = new JTextField();
		joinFiltersTextField.setBounds(10, 260, 318, 20);
		joinFiltersTextField.setToolTipText("eg: Month > 5 and name = 'Joe' ");
		rightPanel.add(joinFiltersTextField);
		joinFiltersTextField.setColumns(10);
		
		JLabel joinTypesLabel = new JLabel("Join Types");
		joinTypesLabel.setBounds(10, 291, 70, 14);
		rightPanel.add(joinTypesLabel);
		
		joinTypesTextField = new JTextField();
		joinTypesTextField.setBounds(10, 316, 318, 20);
		joinTypesTextField.setToolTipText("eg: Left,Right,Full ");
		rightPanel.add(joinTypesTextField);
		joinTypesTextField.setColumns(10);
		
		JLabel whereFilterLabel = new JLabel("Where Filters");
		whereFilterLabel.setBounds(10, 347, 70, 14);
		rightPanel.add(whereFilterLabel);
		
		whereFilterTextField = new JTextField();
		whereFilterTextField.setBounds(10, 372, 318, 20);
		whereFilterTextField.setToolTipText("eg: Month > 5 and name = 'Joe' ");
		rightPanel.add(whereFilterTextField);
		whereFilterTextField.setColumns(10);
		
		this.queryRunner = new JButton("Run Query");
		queryRunner.addActionListener(new ActionListener() { 
		    public void actionPerformed(ActionEvent e) {
		    	try {
			    	long start = System.currentTimeMillis();
		    		SimpleClientApp client = new SimpleClientApp();
		    		client.queryRunner();
					long end = System.currentTimeMillis();
					float sec = (end - start) / 1000F; 
					System.out.println(sec + " seconds");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
		    }
		});
		queryRunner.setBounds(205, 400, 123, 23);
		rightPanel.add(queryRunner);
		
		JButton customQueryButton = new JButton("Custom Query");
		customQueryButton.addActionListener(new ActionListener() { 
		    public void actionPerformed(ActionEvent e) {
		    	CustomQueryFrame customQM = new CustomQueryFrame();
		    	customQM.setVisible(true);
		    }
		});
		customQueryButton.setBounds(10, 400, 123, 23);
		rightPanel.add(customQueryButton);
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
			} catch (AnalysisException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void createJTables(String fileAlias, Dataset<Row> df) {
		ArrayList<String[]> dataOfFile = new ArrayList<String[]>();
		String nameOfTable[] = {fileAlias,"Type"};
		for(Tuple2<String, String> element : df.dtypes()) {
			String[] s = {element._1, element._2.substring(0, element._2.length()-4)};
			dataOfFile.add(s);
		}
		String tableData[][] = new String[dataOfFile.size()][];
		for(int i=0;i<dataOfFile.size();i++) {
			tableData[i] = dataOfFile.get(i);
		}
		JPanel panel = new JPanel();
		JTable jt = new JTable(tableData,nameOfTable);
		Dimension d = jt.getPreferredSize();
		jt.setPreferredScrollableViewportSize(d);
		panel.add(jt);
		panel.validate();
		JScrollPane sp = new JScrollPane(jt);
		panel.add(sp);
		mainw.getLeftPanel().add(panel);
		SwingUtilities.updateComponentTreeUI(mainw.getFrame());
		dataOfFile.clear();
	}
	
	private void wipeMethod() throws IOException {
		SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		List<StructuredFile> fileList = schMan.getFileList();
		for(StructuredFile sf: fileList) {
			spark.catalog().dropGlobalTempView(sf.getSfAlias());
		}
		schMan.wipeFileList();
		schMan.wipeRepoFile();
		SwingUtilities.updateComponentTreeUI(mainw.getFrame());
		Component[] components = mainw.getLeftPanel().getComponents();
        for (Component component : components) {
        	mainw.getLeftPanel().remove(component);
        }
		SwingUtilities.updateComponentTreeUI(mainw.getFrame());
	}
}
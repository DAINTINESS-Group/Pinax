package app.simpleClient;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
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
import querymanager.QueryManagerFactory;
import querymanager.QueryManagerInterface;
import scala.Tuple2;
import engine.FunctionManager;
import javax.swing.JLabel;
import javax.swing.JTextField;


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
	private JTextField primaryTableTextField;
		
		
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
		QueryManagerInterface qrMan = new QueryManagerFactory().createQueryManager();

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
		frame.setBounds(50, 25, 600, 450);
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
		primaryTableTextField.setBounds(10, 36, 201, 20);
		rightPanel.add(primaryTableTextField);
		primaryTableTextField.setColumns(10);
		
		JButton btnNewButton = new JButton("Run Query");
		openItem.addActionListener(func.createCommand("Run"));
		btnNewButton.setBounds(255, 350, 123, 23);
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
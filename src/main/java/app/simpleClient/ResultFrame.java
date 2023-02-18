package app.simpleClient;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.EmptyBorder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.opencsv.CSVWriter;

import scala.Tuple2;
import javax.swing.JButton;
import javax.swing.JTextField;

@SuppressWarnings("all")
public class ResultFrame extends JFrame {

	private JPanel contentPane;
	private JScrollPane scrollPane;
	private JTextField fileName;
	Dataset<Row> results = null;

	/**
	 * Create the frame.
	 */
	public ResultFrame() {
		setTitle("Results");
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setBounds(100, 100, 400, 500);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(new BorderLayout(0, 0));
		
		JButton saveItem = new JButton("Save As CSV File");
		saveItem.addActionListener(new ActionListener() { 
		    public void actionPerformed(ActionEvent e) {
		    	createCSVFile();
		    }
		});
		contentPane.add(saveItem, BorderLayout.SOUTH);
		
		fileName = new JTextField();
		contentPane.add(fileName, BorderLayout.NORTH);
		fileName.setColumns(10);
	}
	
	public void createResultJTable(Dataset<Row> results) {
		this.results = results;
		ArrayList<String[]> namesOfColumns = new ArrayList<String[]>();
		ArrayList<String[]> dataOfFile = new ArrayList<String[]>();
		for(Tuple2<String, String> element : results.dtypes()) {
			String s[] = {element._1};
			namesOfColumns.add(s);
		}
		List<Row> resultList = results.collectAsList();
		for(Row r: resultList) {
			String arr[] = {r.toString()};
			dataOfFile.add(arr);
		}
		String tableData[][] = new String[dataOfFile.size()][];
		for(int i=0;i<dataOfFile.size();i++) {
			String arr[] = dataOfFile.get(i)[0].replaceAll("[\\[ \\]]", "").split(",");
			tableData[i] = arr;
		}
		JTable jt = new JTable(tableData,exportArrayFromList(namesOfColumns));
		scrollPane = new JScrollPane(jt, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		Dimension d = jt.getPreferredSize();
		jt.setPreferredScrollableViewportSize(d);
		contentPane.add(scrollPane);
		namesOfColumns.clear();
		dataOfFile.clear();
	}
	
	public String[] exportArrayFromList(ArrayList<String[]> lst)  
	{  
	    String[] array = new String[lst.size()];  
	    for(int i=0;i<lst.size();i++)
	    {  
	    	array[i] = lst.get(i)[0];
	    }  
	    return array;  
	}
	
	public void createCSVFile() {
		if(fileName.getText() != null && !fileName.getText().trim().isEmpty()) {
			ArrayList<String[]> dataOfFile = new ArrayList<String[]>();
			String names = "";
			int counter = 0;
			for(Tuple2<String, String> element : results.dtypes()) {
				if(counter == 0) {
					names += element._1;
					counter ++;
				}
				else {
					names += "," + element._1;
				}
			}
			String namesArray[] = names.split(",");
			dataOfFile.add(namesArray);
			List<Row> resultList = results.collectAsList();
			for(Row r: resultList) {
				String data = r.toString();
				String dataArray[] = data.replaceAll("[\\[ \\]]", "").split(",");
				dataOfFile.add(dataArray);
			}
			String path = "src/main/resources/saved/" + fileName.getText() + ".csv";
			File file = new File(path);
			try {
				 FileWriter outputfile = new FileWriter(file);
				 CSVWriter writer = new CSVWriter(outputfile);
				 writer.writeAll(dataOfFile);
				 writer.close();
			 }
			 catch (IOException e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 }
		}
		else {
			JOptionPane.showMessageDialog(contentPane, "Name is empty. Please type a name.");
		}
	}
}
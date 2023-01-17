package app.simpleClient;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.EmptyBorder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

@SuppressWarnings("all")
public class ResultFrame extends JFrame {

	private JPanel contentPane;
	private JScrollPane scrollPane;

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
	}
	
	public void createResultJTable(Dataset<Row> results) {
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

}
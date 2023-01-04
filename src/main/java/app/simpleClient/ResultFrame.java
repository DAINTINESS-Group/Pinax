package app.simpleClient;

import java.awt.BorderLayout;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ResultFrame extends JFrame {

	private JPanel contentPane;

	/**
	 * Create the frame.
	 */
	public ResultFrame(Dataset<Row> df) {
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
	}

}
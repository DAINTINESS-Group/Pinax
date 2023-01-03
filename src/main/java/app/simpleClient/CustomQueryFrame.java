package app.simpleClient;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.InputMismatchException;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.EmptyBorder;
import javax.swing.JButton;
import javax.swing.JTextArea;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

@SuppressWarnings("all")
public class CustomQueryFrame extends JFrame {

	private JPanel contentPane;
	private JTextArea textArea;
	private JLabel lblNewLabel;

	/**
	 * Create the frame.
	 */
	public CustomQueryFrame() {
		setTitle("Custom Query");
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setBounds(100, 100, 500, 200);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		
		textArea = new JTextArea();
		contentPane.add(textArea, BorderLayout.CENTER);
		
		JScrollPane scrollPane = new JScrollPane(textArea);
		contentPane.add(scrollPane);
		
		JButton runButton = new JButton("Run Custom Query");
		runButton.addActionListener(new ActionListener() {
		    public void actionPerformed(ActionEvent e) {
		    	if(textArea.getText() != null && !textArea.getText().trim().isEmpty()) {
		    		try {
			    		SimpleClientApp client = new SimpleClientApp();
				    	client.runCustomQuery(textArea.getText());
		    		}
		    		catch (Exception m) {
		    			JOptionPane.showMessageDialog(contentPane, "Something went wrong. Check the query for errors.");
		    		}
		    	}
		    	else {
		    		JOptionPane.showMessageDialog(contentPane, "Query is empty. Please type a query.");
		    	}
		    }
		});
		contentPane.add(runButton, BorderLayout.SOUTH);
		
		lblNewLabel = new JLabel("Add \"global_temp.\" before typing the name of any table.");
		contentPane.add(lblNewLabel, BorderLayout.NORTH);
	}
	
}

package app.simpleClient;

import java.awt.BorderLayout;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingConstants;
import javax.swing.JLabel;
import java.awt.FlowLayout;

@SuppressWarnings("all")
public class HelpFrame extends JFrame {

	private JPanel contentPane;
	private static JPanel panel = new JPanel();

	/**
	 * Create the frame.
	 */
	public HelpFrame() {
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		setTitle("Help Frame");
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
	    panel.setBorder(LineBorder.createBlackLineBorder());

	    JScrollPane scrollPane = new JScrollPane(panel,   ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
	    panel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
	    
	    JLabel instructionsLabel = new JLabel("<html>"
	    		+ "\r\nThis are some instructions on how to use this tool properly."
	    		+ "<br/>\r\n1. Always add a primary table."
	    		+ "<br/>\r\n2. Split everything with a \",\"."
	    		+ "<br/>\r\n3. Join filters,table aliases and join types always must be in the same order as the tables that will be joined."
	    		+ "<br/>\r\n4. The first alias is used for the primary table."
	    		+ "<br/>\r\n5. In the attributes list, you need to add the table alias. "
	    		+ "For example if you give a table the alias p then you must type p.whatever in case the whatever attribute is in more than one tables."
	    		+ "<br/>\r\n6. In the where and join filters if you want to compare with a string, always place the string inside quotation marks."
	    		+ "For example \" Month = 'January' and Month = 'July' \" where month is the name of the attribute.</br>\r\n"
	    		+ "<br/>\r\n7. All the special characters in the file name are turned into '_'(underscore). "
	    		+ "When using the said files use _ instead of any character they have in their name.</br>\r\n"
	    		+ "<br/>\r\n8. You don't have to add the primary table to the join list text field."
	    		+ "</html>");
		instructionsLabel.setVerticalAlignment(SwingConstants.TOP);
		instructionsLabel.setHorizontalAlignment(SwingConstants.LEFT);
		instructionsLabel.setBounds(333, 39, 195, 353);

	    panel.add(instructionsLabel);
	    contentPane.add(scrollPane); // add acrollpane to frame
	}
}

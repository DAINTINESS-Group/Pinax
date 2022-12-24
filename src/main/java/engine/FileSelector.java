package engine;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.filechooser.FileSystemView;

import app.simpleClient.UserInterface;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileSelector implements ActionListener {
	
	private File[] files;
	private UserInterface mainw = UserInterface.getSingletonView();

	public void filterFile(JFileChooser jfc) {
		FileNameExtensionFilter filter = new FileNameExtensionFilter("tsv,csv Files", "csv", "tsv");
		jfc.setAcceptAllFileFilterUsed(false);
		jfc.setFileFilter(filter);
	}
	
	public boolean pickFile() throws Exception{
		SchemaManagerInterface schMan = new SchemaManagerFactory().createSchemaManager();
		JFileChooser jfc = new JFileChooser(FileSystemView.getFileSystemView().getDefaultDirectory());
		filterFile(jfc);
		jfc.setMultiSelectionEnabled(true);
		int returnValue = jfc.showOpenDialog(mainw.getFrame());
		if (returnValue == JFileChooser.APPROVE_OPTION) {
			files = jfc.getSelectedFiles();
			for(File file : files) {
			    Path absolutePath = Paths.get(file.getAbsolutePath());
			    String fileAlias = schMan.createFileAlias(absolutePath.getFileName().toString());
				String fileType = schMan.getFileType(absolutePath.getFileName().toString());
				schMan.registerFileAsDataSource(fileAlias, absolutePath, fileType);
			}
			return true;
		}
		else {
			System.out.println("No files were selected");
		}
		return false;
	}
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		try {
			if(pickFile()) {
				mainw.loadFiles();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

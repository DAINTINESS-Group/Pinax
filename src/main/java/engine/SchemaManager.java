package engine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import model.FileColumn;
import model.StructuredFile;
/**
 * Schema manager is 	INDEPENDENT    of anything in the front-end package
 * You have started the wrong way with this: you started with the front-end
 * You need to start with the back-end!!
 * 
 * @author pvassil
 *
 */
class SchemaManager implements SchemaManagerInterface{
	private static final String _REGISTERED_FILE_REPO = "src/main/resources/_REPO/RegisteredFiles.tsv";
	
	/**
	 * The list of files that are already registered in the _REPO + those who are to be added.
	 * The first time the schemaManager is created the fileList immediately loads all the Repo file contents.
	 */
	private List<StructuredFile> fileList;

	public SchemaManager() throws IOException {
		this.fileList = new ArrayList<StructuredFile>();
		updateFileList();
		//TODO populate the list with the contents of the file repo
	}
	
	public List<StructuredFile> getFileList(){
		return fileList;
	}
	
	/**
	 * Cleans up the internal list of files of the Schema Manager
	 * 
	 * @return the size of the fileList
	 */
	public int wipeFileList() {
		fileList.clear();
		return fileList.size();
	}
	
	/**
	 * Creates the alias that is used in the registration of the file
	 * 
	 */
	public String createFileAlias(String s) {
		File filePath = new File(s);
		String name = filePath.getName();
		name = name.substring(0,name.lastIndexOf("."));
		name = name.replaceAll("[^a-zA-Z0-9]", "_");  
		return name;
	}
	
	/**
	 * Gets the file type.
	 * 
	 */
	public String getFileType(String s) {
		File filePath = new File(s);
		String type = filePath.getName();
		type = type.substring(type.lastIndexOf(".")+1,type.length());
		return type;
	}
	
	/**
	 * Creates a list of filecolumn items, that is used to create the structured file.
	 * 
	 * @param path path of the file
	 * @param delimiter 
	 * @return the created list
	 * @throws IOException 
	 */
	public List<FileColumn> getColumns(String path, String delimiter) throws IOException {
		List<FileColumn> fileColumns = new ArrayList<FileColumn>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			String line = br.readLine();
			String line2 = br.readLine();
			String[] columns = line.split(delimiter);
			String[] temp = line2.split(delimiter);
			for(int i=0;i<columns.length;i++) {
				String name = "";
				String type = "";
				if(columns[i] != null && !columns[i].trim().isEmpty()) {
					name = columns[i];
				}
				if(temp[i] != null && !temp[i].trim().isEmpty()) {
					type = temp[i].getClass().getSimpleName();
				}
				int position = i;
				FileColumn tempColumn = new FileColumn(name,type,position);
				fileColumns.add(tempColumn);
			}
		}
		return fileColumns;
	}
	
	/**
	 * Fills the file list with the contents of the Repo file.
	 * 
	 * @return
	 * @throws IOException
	 */
	public int updateFileList() throws IOException {
		List<String[]> fileContents = getRepoFileContents();
		fileContents = fileContents.subList(1,fileContents.size());
		for(String[] content : fileContents) {
			Path path = Paths.get(content[1]);
			File file = new File(path.toString());
			if(file.exists()) {
				String fileAlias = content[0];
				String fileType = content[2];
				String delimiter = delimiterSelector(fileType);
				List<FileColumn> columnList = getColumns(path.toString(), delimiter);
				StructuredFile structFileItem = new StructuredFile(fileAlias,path,columnList,fileType);
				fileList.add(structFileItem);
			}
		}
		return fileList.size();
	}
	
	/**
	 * Adds a new file in the collections of registered files; also writes it into the repo file.
	 * 
	 * @param fileAlias a String with a common alias for the new file.
	 * @param path a Path for the new file
	 * @return 0 if all is OK; negative numbers depending on the problems encountered
	 * @throws IOException 
	 */
	public int registerFileAsDataSource(String fileAlias, Path path, String fileType) {
		//TODO complete the code
		//check if the alias is used already
		for(StructuredFile iter: fileList) {
			if(iter.getSfAlias().equals(fileAlias)) {
				System.out.println("alias used already");
				return -1;
			}
		}
		//check if there is indeed a file in the path
		if(!path.toFile().exists()) {
			System.out.println("the file does not exist");
			return -2;
		}
		//check its columns
		//add path to the this.FileList
		//write its info in the _REGISTERED_FILE_REPO
		try {
			String delimiter = delimiterSelector(fileType);
			List<FileColumn> columnList = getColumns(path.toString(), delimiter);
			StructuredFile structFileItem = new StructuredFile(fileAlias,path,columnList,fileType);
			fileList.add(structFileItem);
			fillRepoFile();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -3;
		}
		//return 0 if all is well
		return 0;
	}//end registerFileAsDataSource
	
	/**
	 * Returns a StructuredFile that has the prescribed alias name, if such a file has been registered;
	 * null otherwise
	 * 
	 * @param pAlias
	 * @return
	 * @throws CsvException 
	 * @throws IOException 
	 */
	public StructuredFile getFileByAliasName(String pAlias) {
		for(StructuredFile sf: fileList) {
			if(sf.getSfAlias().equals(pAlias)) {
				return sf;
			}
		}
		//TODO complete the code
		return null;
	}
	
	/**
	 * Clears the contents of the Repo file.
	 */
	public long wipeRepoFile() throws IOException {
		File repoFile = new File(_REGISTERED_FILE_REPO);
		
		FileWriter outputfile = new FileWriter(repoFile,false);
		CSVWriter writer = new CSVWriter(outputfile,'\t',CSVWriter.NO_QUOTE_CHARACTER,CSVWriter.DEFAULT_ESCAPE_CHARACTER,CSVWriter.DEFAULT_LINE_END);
		String[] header = {"RegisteredFileAlias", "Path", "Type"};
		writer.writeNext(header);
		writer.close();
		Path path = Paths.get(_REGISTERED_FILE_REPO);
		long lines = Files.lines(path).count();
		return lines;
	}
	
	/**
	 * Fills the Repo file with the fileList contents, if the file does not already contain them.
	 * @throws IOException 
	 */
	private int fillRepoFile() throws IOException {
		File repoFile = new File(_REGISTERED_FILE_REPO);
		FileWriter outputfile = new FileWriter(repoFile,true);
		CSVWriter writer = new CSVWriter(outputfile,'\t',CSVWriter.NO_QUOTE_CHARACTER,CSVWriter.DEFAULT_ESCAPE_CHARACTER,CSVWriter.DEFAULT_LINE_END);
		for(StructuredFile sf: fileList) {
			if(!checkIfAliasInRepoFile(sf.getSfAlias())) {
				String str = "";
				str += sf.getSfAlias();
				str += ",";
				str += sf.getSfPath();
				str += ",";
				str += sf.getSfType();
				String[] contents = str.split(",");
				writer.writeNext(contents);
			}
		}
		writer.close();
		return 1;
	}
	
	public List<String[]> getRepoFileContents() throws IOException {
		List<String[]> data = new ArrayList<>(); //initializing a new List out of String[]'s
		String line = null;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(_REGISTERED_FILE_REPO));
		while ((line = bufferedReader.readLine()) != null) {
            String[] lineItems = line.split("\t"); //splitting the line and adding its items in String[]
            data.add(lineItems); //adding the splitted line array to the ArrayList
        }
		bufferedReader.close();
	    return data;
	}
	
	/**
	 * Returns true if the alias is used inside the Repo file.
	 * @param alias
	 * @return
	 * @throws IOException 
	 */
	private boolean checkIfAliasInRepoFile(String alias) throws IOException {
		List<String[]> fileContents = getRepoFileContents();
		if(fileContents.size()>1) {
			fileContents = fileContents.subList(1,fileContents.size());
		}
		else {
			return false;
		}
		for(String[] content : fileContents) {
			String checker = content[0];
			if(checker.equals(alias)) {
				return true;
			}
		}
		return false;
	}
	
	public String delimiterSelector(String fileType) {
		String delimiter = "";
		if(fileType.equals("tsv")) {
			delimiter = "\t";
		}
		else if(fileType.equals("csv")) {
			delimiter = ",";
		}
		else if(fileType.equals("txt")) {
			delimiter = " ";
		}
		return delimiter;
	}
	
}//end class

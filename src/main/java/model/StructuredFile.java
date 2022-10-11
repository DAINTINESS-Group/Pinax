package model;

import java.nio.file.Path;
import java.util.List;
import java.util.HashMap;

/**
 * The class represents a Structured File. It comprises
 * 
 * @author pvassil
 *
 */
public class StructuredFile {
	/**
	 * A String that is the nickname via which the user uses the file
	 */
	private String sfAlias;
	
	/**
	 * The Path to the actual file
	 */
	private Path sfPath;
	
	/**
	 * The List of columns of the path. Each column must be placed in the correct position
	 */
	private List<FileColumn> sfColumns;
	
	/**
	 * A complementary Hashmap to the list of columns. Once the list of columns has been populated,
	 * you also add this smart "index" to lookup FileColumn objects by name of the column.
	 */
	private HashMap<String, FileColumn> sfColumnMap;
	
	/**
	 * The type of the file. Helps with choosing the right delimiter for the creation of the DF.
	 */
	private String type;
	
	/**
	 * The main constructor for a StructuredFile object
	 * @param sfAlias
	 * @param sfPath
	 * @param sfColumns
	 */
	public StructuredFile(String sfAlias, Path sfPath, List<FileColumn> sfColumns,String type) {
		super();
		this.sfAlias = sfAlias;
		this.sfPath = sfPath;
		this.sfColumns = sfColumns;
		this.type = type;
		
		this.sfColumnMap = new HashMap<String, FileColumn>();
		for(FileColumn fc: this.sfColumns) {
			this.sfColumnMap.put(fc.getName(), fc);
		}
	}

	public String getSfAlias() {
		return sfAlias;
	}

	public Path getSfPath() {
		return sfPath;
	}
	
	public  HashMap<String, FileColumn> getSfColumns() {
		return sfColumnMap;
	}
	
	public String getSfType() {
		return type;
	}
}//end class
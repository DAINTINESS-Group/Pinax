package engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.opencsv.exceptions.CsvException;

import model.StructuredFile;

public interface SchemaManagerInterface {
	
	/**
	 * Cleans up the internal list of files of the Schema Manager
	 * 
	 * @return the size of the fileList
	 */
	public int wipeFileList();
	
	/**
	 * Adds a new file in the collections of registered files; also writes it into the repo file.
	 * 
	 * @param fileAlias a String with a common alias for the new file.
	 * @param path a Path for the new file
	 * @return 0 if all is OK; negative numbers depending on the problems encountered
	 * @throws IOException 
	 */
	public int registerFileAsDataSource(String fileAlias, Path path, String fileType);
	
	/**
	 * Clears the contents of the Repo file.
	 */
	public long wipeRepoFile() throws IOException;
	
	/**
	 * Returns a StructuredFile that has the prescribed alias name, if such a file has been registered;
	 * null otherwise
	 * 
	 * @param pAlias
	 * @return
	 * @throws CsvException 
	 * @throws IOException 
	 */
	public StructuredFile getFileByAliasName(String pAlias);

	/**
	 * Creates the alias that is used in the registration of the file
	 * 
	 * @return the size of the fileList
	 */
	public String createFileAlias(String s);
	
	/**
	 * Gets the file type.
	 * 
	 */
	public String getFileType(String s);
	
	/**
	 * Simple accessor method for the fileList field.
	 * 
	 */
	public List<StructuredFile> getFileList();
	
	/**
	 * Selects the proper delimiter depending on the file type given.
	 * 
	 */
	public String delimiterSelector(String fileType);
	
	public String createNaiveQueryExpression(String primaryTableString);
	
	public String createProjectionOnlyQueryExpression(String primaryTableString, List<String> createTFList);

	public String createProjectSelectSingleTableQueryExpression(String primaryTableString, String tableAliasString,
			List<String> createTFList, String whereFilterString);

	public String createMultiTableQueryExpression(String primaryTableString, List<String> createTFList,
			List<String> createTFList2, List<String> createTFList3, List<String> createTFList4,
			List<String> createTFList5, String whereFilterString);
}

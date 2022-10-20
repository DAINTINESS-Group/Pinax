package engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import model.StructuredFile;

public interface SchemaManagerInterface {
	
	//MUST ADD DOCUMENTATION FULLY
	
	public int registerFileAsDataSource(String fileAlias, Path path, String fileType);
	public long wipeRepoFile() throws IOException;
	public StructuredFile getFileByAliasName(String pAlias);

	
	//WHY WAS ALL THIS USED WITHOUT BEING PART OF THE INTERFACE? OEO?
	public int wipeFileList();
	public String createFileAlias(String s);
	public String getFileType(String s);
	public List<StructuredFile> getFileList();
}

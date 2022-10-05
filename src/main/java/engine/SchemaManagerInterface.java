package engine;

import java.io.IOException;
import java.nio.file.Path;

import model.StructuredFile;

public interface SchemaManagerInterface {
	
	public int registerFileAsDataSource(String fileAlias, Path path, String fileType);
	public long wipeRepoFile() throws IOException;
	public StructuredFile getFileByAliasName(String pAlias);

}

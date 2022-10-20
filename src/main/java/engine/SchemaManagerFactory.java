package engine;

import java.io.IOException;

public class SchemaManagerFactory {

	public SchemaManagerInterface createSchemaManager() throws IOException {
		return new SchemaManager();
	}
}

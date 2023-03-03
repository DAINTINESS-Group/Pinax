## <div align="center">Pinax</div>

#### <div align="center">Java tool that manages structured files as data sources with a schema.</div>

A structured file is a kind of text file, with lines, where each line is a record, the fields of which are separated by a
separator (eg. tabs, comma, pipe, etc). After registering a data set, the system creates a table that shows the names of 
the file's columns and their type, making it easier for the user to construct and sql query.

### <div align="center">Setup</div>

#### Eclipse Installation Requirements

- Install [**Eclipse**](https://www.eclipse.org/downloads/)
- Import the project as a Maven project.

#### Apache Spark Installation Requirements

- Install [**Spark**](https://spark.apache.org/downloads.html)

#### Maven

The project uses a Maven wrapper so there is no need to install it to your system as long as you have the JAVA_HOME environmental 
variable pointing to your [**Java 8**](https://www.oracle.com/java/technologies/downloads/archive/) installation and SPARK_HOME
enviromental variable pointing to your [**Spark**](https://spark.apache.org/downloads.html) installation
folder.

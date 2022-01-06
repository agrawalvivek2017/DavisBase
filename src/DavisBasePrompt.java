import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static java.lang.System.out;

public class DavisBasePrompt {

	static String prompt = "sqlondon> ";
	static String version = "v3.0";
	static String copyright = "London Team";
	static boolean isExit = false;

	static long pageSize = 512;

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	// Main method
	public static void main(String[] args) {

		splashScreen();

		File data_directory = new File("data");

		if (!new File(data_directory, DavisBaseBinaryFile.tables_table + ".tbl").exists()
				|| !new File(data_directory, DavisBaseBinaryFile.columns_table + ".tbl").exists())
			DavisBaseBinaryFile.initialize_data_storage();
		else
			DavisBaseBinaryFile.is_data_store_initialized = true;

		String userCommand = "";

		while (!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}

	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to SQLLondon");
		System.out.println("SQLondon Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself
	 *         num times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}

	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}

	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");

		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");

		out.println("CREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique>);");
		out.println("\tCreates a table with the given columns.\n");

		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");

		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");

		out.println("INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");
		out.println("\tInserts a new record into the table with the given values for the given columns.\n");

		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");

		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");

		out.println("HELP;");
		out.println("\tDisplay this help information.\n");

		out.println("EXIT;");
		out.println("\tExit the program.\n");

		out.println(line("*", 80));
	}

	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("SQLLondon Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {

		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
		case "show":
			if (commandTokens.get(1).equals("tables"))
				parseUserCommand("select * from davisbase_tables");
			else if (commandTokens.get(1).equals("rowid")) {
				DavisBaseBinaryFile.showing_rowid = true;
				System.out.println("* Table will now display the contents of RowId.");
			} else
				System.out.println("! I didn't understand the command: \"" + userCommand + "\"");
			break;
		case "select":
			parse_query(userCommand);
			break;
		case "drop":
			drop_table(userCommand);
			break;
		case "create":
			if (commandTokens.get(1).equals("table"))
				parse_create_table(userCommand);
			else if (commandTokens.get(1).equals("index"))
				parse_create_index(userCommand);
			break;
		case "update":
			parse_update(userCommand);
			break;
		case "insert":
			parse_insert(userCommand);
			break;
		case "delete":
			parse_delete(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
			break;
		case "test":
			test();
			break;
		default:
			System.out.println("! I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	public static void test() {
		Scanner scan = new Scanner(System.in);
		parseUserCommand("create table test (id int, name text)");
		scan.nextLine();
		parseUserCommand("create index on test (name)");
		scan.nextLine();
		for (int i = 1; i < 35; i++) {

			parseUserCommand("insert into test (id , name) values (" + (i) + ", " + i + "'arun' )");

		}
		parseUserCommand("show tables");

		scan.nextLine();
		scan.close();

	}

	public static void parse_create_index(String createIndexString) {
		ArrayList<String> create_index_tokens = new ArrayList<String>(Arrays.asList(createIndexString.split(" ")));
		try {
			if (!create_index_tokens.get(2).equals("on") || !createIndexString.contains("(")
					|| !createIndexString.contains(")") && create_index_tokens.size() < 4) {
				System.out.println("Incorrect Syntax");
				return;
			}

			String table_name = createIndexString
					.substring(createIndexString.indexOf("on") + 3, createIndexString.indexOf("(")).trim();
			String column_name = createIndexString
					.substring(createIndexString.indexOf("(") + 1, createIndexString.indexOf(")")).trim();

			if (new File(DavisBasePrompt.getNDXFilePath(table_name, column_name)).exists()) {
				System.out.println("Index already there");
				return;
			}

			RandomAccessFile table_file = new RandomAccessFile(getTBLFilePath(table_name), "rw");

			TableMetaData meta_data = new TableMetaData(table_name);

			if (!meta_data.is_table_existing) {
				System.out.println("Incorrect Table name");
				table_file.close();
				return;
			}

			int column_ordinal = meta_data.column_names.indexOf(column_name);

			if (column_ordinal < 0) {
				System.out.println("Incorrect column name");
				table_file.close();
				return;
			}

			RandomAccessFile index_file = new RandomAccessFile(getNDXFilePath(table_name, column_name), "rw");
			Page.add_new_pg(index_file, PageType.LEAFINDEX, -1, -1);
			if (meta_data.record_count > 0) {
				BPlusTree bPlusOneTree = new BPlusTree(table_file, meta_data.root_page_number, meta_data.table_name);
				for (int pageNo : bPlusOneTree.get_allLeaves()) {
					Page page = new Page(table_file, pageNo);
					BTree bTree = new BTree(index_file);
					for (TableRecord record : page.get_pg_records()) {
						bTree.insert(record.getAttributes().get(column_ordinal), record.row_id);
					}
				}
			}

			System.out.println("Index created on the column : " + column_name);
			index_file.close();
			table_file.close();

		} catch (IOException e) {

			System.out.println("Unable to create Index");
			System.out.println(e);
		}

	}

	public static void drop_table(String dropTableString) {
		System.out.println("STUB: This is the drop_table method.");
		System.out.println("\tParsing the string:\"" + dropTableString + "\"");

		String[] tokens = dropTableString.split(" ");
		if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
			System.out.println("Error");
			return;
		}

		ArrayList<String> drop_table_tokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
		String table_name = drop_table_tokens.get(2);

		parse_delete(
				"delete from table " + DavisBaseBinaryFile.tables_table + " where table_name = '" + table_name + "' ");
		parse_delete(
				"delete from table " + DavisBaseBinaryFile.columns_table + " where table_name = '" + table_name + "' ");
		File table_file = new File("data/" + table_name + ".tbl");
		if (table_file.delete()) {
			System.out.println("table dropped");
		} else
			System.out.println("table not found");

		File f = new File("data/");
		File[] matchingFiles = f.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(table_name) && name.endsWith("ndx");
			}
		});
		boolean iFlag = false;
		for (File file : matchingFiles) {
			if (file.delete()) {
				iFlag = true;
				System.out.println("index deleted");
			}
		}
		if (iFlag)
			System.out.println("drop " + table_name);
		else
			System.out.println("index not found");

	}

	public static void parse_query(String queryString) {
		String table_name = "";
		List<String> column_names = new ArrayList<String>();

		ArrayList<String> query_table_tokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
		int i = 0;

		for (i = 1; i < query_table_tokens.size(); i++) {
			if (query_table_tokens.get(i).equals("from")) {
				++i;
				table_name = query_table_tokens.get(i);
				break;
			}
			if (!query_table_tokens.get(i).equals("*") && !query_table_tokens.get(i).equals(",")) {
				if (query_table_tokens.get(i).contains(",")) {
					ArrayList<String> colList = new ArrayList<String>(
							Arrays.asList(query_table_tokens.get(i).split(",")));
					for (String col : colList) {
						column_names.add(col.trim());
					}
				} else
					column_names.add(query_table_tokens.get(i));
			}
		}

		TableMetaData tableMetaData = new TableMetaData(table_name);
		if (!tableMetaData.is_table_existing) {
			System.out.println("Table does not exists");
			return;
		}

		Condition condition = null;
		try {

			condition = condition_extraction_from_query(tableMetaData, queryString);

		} catch (Exception e) {
			System.out.println(e.getMessage());
			return;
		}

		if (column_names.size() == 0) {
			column_names = tableMetaData.column_names;
		}
		try {

			RandomAccessFile table_file = new RandomAccessFile(getTBLFilePath(table_name), "r");
			DavisBaseBinaryFile tableBinaryFile = new DavisBaseBinaryFile(table_file);
			tableBinaryFile.select_records(tableMetaData, column_names, condition);
			table_file.close();
		} catch (IOException exception) {
			System.out.println("Unable to select fields from table");
		}

	}

	public static void parse_update(String updateString) {
		ArrayList<String> update_tokens = new ArrayList<String>(Arrays.asList(updateString.split(" ")));

		String table_name = update_tokens.get(1);
		List<String> cols_to_update = new ArrayList<>();
		List<String> val_to_update = new ArrayList<>();

		if (!update_tokens.get(2).equals("set") || !update_tokens.contains("=")) {
			System.out.println("! Syntax error !");
			System.out.println(
					"Expected Syntax: UPDATE [table_name] SET [Column_name] = val1 where [column_name] = val2;");
			return;
		}

		String update_col_info_string = updateString.split("set")[1].split("where")[0];

		List<String> column_newValueSet = Arrays.asList(update_col_info_string.split(","));

		for (String item : column_newValueSet) {
			cols_to_update.add(item.split("=")[0].trim());
			val_to_update.add(item.split("=")[1].trim().replace("\"", "").replace("'", ""));
		}

		TableMetaData metadata = new TableMetaData(table_name);

		if (!metadata.is_table_existing) {
			System.out.println("Invalid Table name");
			return;
		}

		if (!metadata.is_column_existing(cols_to_update)) {
			System.out.println("Invalid column name(s)");
			return;
		}

		Condition condition = null;
		try {

			condition = condition_extraction_from_query(metadata, updateString);

		} catch (Exception e) {
			System.out.println(e.getMessage());
			return;

		}

		try {
			RandomAccessFile file = new RandomAccessFile(getTBLFilePath(table_name), "rw");
			DavisBaseBinaryFile binaryFile = new DavisBaseBinaryFile(file);
			int noOfRecordsupdated = binaryFile.update_records_operation(metadata, condition, cols_to_update,
					val_to_update);
			if (noOfRecordsupdated > 0) {
				List<Integer> allRowids = new ArrayList<>();
				for (ColumnInformation colInfo : metadata.column_name_attributes) {
					for (int i = 0; i < cols_to_update.size(); i++)
						if (colInfo.col_name.equals(cols_to_update.get(i)) && colInfo.has_index) {
							if (condition == null) {
								if (allRowids.size() == 0) {
									BPlusTree bPlusOneTree = new BPlusTree(file, metadata.root_page_number,
											metadata.table_name);
									for (int pageNo : bPlusOneTree.get_allLeaves()) {
										Page currentPage = new Page(file, pageNo);
										for (TableRecord record : currentPage.get_pg_records()) {
											allRowids.add(record.row_id);
										}
									}
								}
								RandomAccessFile indexFile = new RandomAccessFile(
										getNDXFilePath(table_name, cols_to_update.get(i)), "rw");
								Page.add_new_pg(indexFile, PageType.LEAFINDEX, -1, -1);
								BTree bTree = new BTree(indexFile);
								bTree.insert(new Attribute(colInfo.data_type, val_to_update.get(i)), allRowids);
							}
						}
				}
			}

			file.close();

		} catch (Exception e) {
			out.println("Unable to update the " + table_name + " file");
			out.println(e);
		}

	}

	public static void parse_insert(String queryString) {
		ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));

		if (!insertTokens.get(1).equals("into") || !queryString.contains(") values")) {
			System.out.println("! Syntax error !");
			System.out.println("Expected Syntax: INSERT INTO table_name ( columns ) VALUES ( values );");

			return;
		}

		try {
			String table_name = insertTokens.get(2);
			if (table_name.trim().length() == 0) {
				System.out.println("Tablename cannot be empty !");
				return;
			}

			// parsing logic
			if (table_name.indexOf("(") > -1) {
				table_name = table_name.substring(0, table_name.indexOf("("));
			}
			TableMetaData dstMetaData = new TableMetaData(table_name);

			if (!dstMetaData.is_table_existing) {
				System.out.println("! Table not exist. !");
				return;
			}

			ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(
					queryString.substring(queryString.indexOf("(") + 1, queryString.indexOf(") values")).split(",")));

			// Column List validation
			for (String colToken : columnTokens) {
				if (!dstMetaData.column_names.contains(colToken.trim())) {
					System.out.println("! Invalid column : !" + colToken.trim());
					return;
				}
			}

			String valuesString = queryString.substring(queryString.indexOf("values") + 6, queryString.length() - 1);

			ArrayList<String> valueTokens = new ArrayList<String>(Arrays
					.asList(valuesString.substring(valuesString.indexOf("(") + 1, valuesString.length()).split(",")));

			// fill attributes to insert
			List<Attribute> attributeToInsert = new ArrayList<>();

			for (ColumnInformation colInfo : dstMetaData.column_name_attributes) {
				int i = 0;
				boolean columnProvided = false;
				for (i = 0; i < columnTokens.size(); i++) {
					if (columnTokens.get(i).trim().equals(colInfo.col_name)) {
						columnProvided = true;
						try {
							String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
							if (valueTokens.get(i).trim().equals("null")) {
								if (!colInfo.is_null_col) {
									System.out.println("! Cannot Insert NULL into !" + colInfo.col_name);
									return;
								}
								colInfo.data_type = DataType.NULL;
								value = value.toUpperCase();
							}
							Attribute attr = new Attribute(colInfo.data_type, value);
							attributeToInsert.add(attr);
							break;
						} catch (Exception e) {
							System.out.println("Incorrect data format for " + columnTokens.get(i) + " values: "
									+ valueTokens.get(i));
							return;
						}
					}
				}
				if (columnTokens.size() > i) {
					columnTokens.remove(i);
					valueTokens.remove(i);
				}

				if (!columnProvided) {
					if (colInfo.is_null_col)
						attributeToInsert.add(new Attribute(DataType.NULL, "NULL"));
					else {
						System.out.println("Unable to Insert NULL into " + colInfo.col_name);
						return;
					}
				}
			}

			// insert attributes to the page
			RandomAccessFile dstTable = new RandomAccessFile(getTBLFilePath(table_name), "rw");
			int dstPageNo = BPlusTree.get_pagenum_for_insertion(dstTable, dstMetaData.root_page_number);
			Page dstPage = new Page(dstTable, dstPageNo);

			int rowNo = dstPage.add_table_row(table_name, attributeToInsert);

			// update Index
			if (rowNo != -1) {

				for (int i = 0; i < dstMetaData.column_name_attributes.size(); i++) {
					ColumnInformation col = dstMetaData.column_name_attributes.get(i);

					if (col.has_index) {
						RandomAccessFile indexFile = new RandomAccessFile(getNDXFilePath(table_name, col.col_name),
								"rw");
						BTree bTree = new BTree(indexFile);
						bTree.insert(attributeToInsert.get(i), rowNo);
					}

				}
			}

			dstTable.close();
			if (rowNo != -1)
				System.out.println("Record Inserted into table");
			System.out.println();

		} catch (Exception ex) {
			System.out.println("Unable to insert the record in table");
			System.out.println(ex);

		}
	}

	/**
	 * Create new table
	 * 
	 * @param queryString
	 *            is a String of the user input
	 */
	public static void parse_create_table(String createTableString) {

		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
		// table and () check
		if (!createTableTokens.get(1).equals("table")) {
			System.out.println("Syntax Error");
			return;
		}
		String table_name = createTableTokens.get(2);
		if (table_name.trim().length() == 0) {
			System.out.println("Tablename cannot be empty");
			return;
		}
		try {

			if (table_name.indexOf("(") > -1) {
				table_name = table_name.substring(0, table_name.indexOf("("));
			}

			List<ColumnInformation> lstcolumnInformation = new ArrayList<>();
			ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(createTableString
					.substring(createTableString.indexOf("(") + 1, createTableString.length() - 1).split(",")));

			short ordinal_pos = 1;

			String primary_key_col = "";

			for (String columnToken : columnTokens) {

				ArrayList<String> colInfoToken = new ArrayList<String>(Arrays.asList(columnToken.trim().split(" ")));
				ColumnInformation colInfo = new ColumnInformation();
				colInfo.table_name = table_name;
				colInfo.col_name = colInfoToken.get(0);
				colInfo.is_null_col = true;
				colInfo.data_type = DataType.get(colInfoToken.get(1).toUpperCase());
				for (int i = 0; i < colInfoToken.size(); i++) {

					if ((colInfoToken.get(i).equals("null"))) {
						colInfo.is_null_col = true;
					}
					if (colInfoToken.get(i).contains("not") && (colInfoToken.get(i + 1).contains("null"))) {
						colInfo.is_null_col = false;
						i++;
					}

					if ((colInfoToken.get(i).equals("unique"))) {
						colInfo.is_unique_col = true;
					} else if (colInfoToken.get(i).contains("primary") && (colInfoToken.get(i + 1).contains("key"))) {
						colInfo.is_key_primary = true;
						colInfo.is_unique_col = true;
						colInfo.is_null_col = false;
						primary_key_col = colInfo.col_name;
						i++;
					}

				}
				colInfo.ordinal_pos = ordinal_pos++;
				lstcolumnInformation.add(colInfo);

			}

			// update sys file
			RandomAccessFile davisbase_catalog_of_tables = new RandomAccessFile(
					getTBLFilePath(DavisBaseBinaryFile.tables_table), "rw");
			TableMetaData davisbaseTableMetaData = new TableMetaData(DavisBaseBinaryFile.tables_table);

			int pageNo = BPlusTree.get_pagenum_for_insertion(davisbase_catalog_of_tables,
					davisbaseTableMetaData.root_page_number);

			Page page = new Page(davisbase_catalog_of_tables, pageNo);

			int rowNo = page.add_table_row(DavisBaseBinaryFile.tables_table,
					Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, table_name), // DavisBaseBinaryFile.tablesTable->test
							new Attribute(DataType.INT, "0"), new Attribute(DataType.SMALLINT, "0"),
							new Attribute(DataType.SMALLINT, "0") }));
			davisbase_catalog_of_tables.close();

			if (rowNo == -1) {
				System.out.println("table Name conflict");
				return;
			}
			RandomAccessFile table_file = new RandomAccessFile(getTBLFilePath(table_name), "rw");
			Page.add_new_pg(table_file, PageType.LEAF, -1, -1);
			table_file.close();

			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
					getTBLFilePath(DavisBaseBinaryFile.columns_table), "rw");
			TableMetaData davisbaseColumnsMetaData = new TableMetaData(DavisBaseBinaryFile.columns_table);
			pageNo = BPlusTree.get_pagenum_for_insertion(davisbaseColumnsCatalog,
					davisbaseColumnsMetaData.root_page_number);

			Page page1 = new Page(davisbaseColumnsCatalog, pageNo);

			for (ColumnInformation column : lstcolumnInformation) {
				page1.add_new_col(column);
			}

			davisbaseColumnsCatalog.close();

			System.out.println("* Table created *");

			if (primary_key_col.length() > 0) {
				parse_create_index("create index on " + table_name + "(" + primary_key_col + ")");
			}
		} catch (Exception e) {

			System.out.println("Unable to create Table");
			System.out.println(e.getMessage());
			parse_delete("delete from table " + DavisBaseBinaryFile.tables_table + " where table_name = '" + table_name
					+ "' ");
			parse_delete("delete from table " + DavisBaseBinaryFile.columns_table + " where table_name = '" + table_name
					+ "' ");
		}

	}

	private static void parse_delete(String deleteTableString) {
		ArrayList<String> del_table_tokens = new ArrayList<String>(Arrays.asList(deleteTableString.split(" ")));

		String table_name = "";

		try {

			if (!del_table_tokens.get(1).equals("from") || !del_table_tokens.get(2).equals("table")) {
				System.out.println("Syntax Error");
				return;
			}

			table_name = del_table_tokens.get(3);

			TableMetaData meta_data = new TableMetaData(table_name);
			Condition condition = null;
			try {
				condition = condition_extraction_from_query(meta_data, deleteTableString);

			} catch (Exception e) {
				System.out.println(e);
				return;
			}
			RandomAccessFile table_file = new RandomAccessFile(getTBLFilePath(table_name), "rw");

			BPlusTree tree = new BPlusTree(table_file, meta_data.root_page_number, meta_data.table_name);
			List<TableRecord> deletedRecords = new ArrayList<TableRecord>();
			int count = 0;
			for (int pageNo : tree.get_allLeaves(condition)) {
				short deleteCountPerPage = 0;
				Page page = new Page(table_file, pageNo);
				for (TableRecord record : page.get_pg_records()) {
					if (condition != null) {
						if (!condition
								.condition_check(record.getAttributes().get(condition.column_ordinal).field_value))
							continue;
					}

					deletedRecords.add(record);
					page.del_table_record(table_name,
							Integer.valueOf(record.pg_head_index - deleteCountPerPage).shortValue());
					deleteCountPerPage++;
					count++;
				}
			}

			if (condition == null) {
		
			} else {
				for (int i = 0; i < meta_data.column_name_attributes.size(); i++) {
					if (meta_data.column_name_attributes.get(i).has_index) {
						RandomAccessFile indexFile = new RandomAccessFile(
								getNDXFilePath(table_name, meta_data.column_name_attributes.get(i).col_name), "rw");
						BTree bTree = new BTree(indexFile);
						for (TableRecord record : deletedRecords) {
							bTree.delete(record.getAttributes().get(i), record.row_id);
						}
					}
				}
			}

			System.out.println();
			table_file.close();
			System.out.println(count + " record(s) deleted!");

		} catch (Exception e) {
			System.out.println("! Error on dropping rows in table : " + table_name);
			System.out.println(e.getMessage());
		}

	}

	public static String getTBLFilePath(String table_name) {
		return "data/" + table_name + ".tbl";
	}

	public static String getNDXFilePath(String table_name, String columnName) {
		return "data/" + table_name + "_" + columnName + ".ndx";
	}

	private static Condition condition_extraction_from_query(TableMetaData tableMetaData, String query)
			throws Exception {
		if (query.contains("where")) {
			Condition condition = new Condition(DataType.TEXT);
			String where_clause = query.substring(query.indexOf("where") + 6, query.length());
			ArrayList<String> where_keyword_tokens = new ArrayList<String>(Arrays.asList(where_clause.split(" ")));

			// WHERE NOT column operator value
			if (where_keyword_tokens.get(0).equalsIgnoreCase("not")) {
				condition.setNegation(true);
			}

			for (int i = 0; i < Condition.supportedOperators.length; i++) {
				if (where_clause.contains(Condition.supportedOperators[i])) {
					where_keyword_tokens = new ArrayList<String>(
							Arrays.asList(where_clause.split(Condition.supportedOperators[i])));
					{
						condition.setOperator(Condition.supportedOperators[i]);
						condition.setConditionValue(where_keyword_tokens.get(1).trim());
						condition.setColumName(where_keyword_tokens.get(0).trim());
						break;
					}

				}
			}
			if (tableMetaData.is_table_existing
					&& tableMetaData.is_column_existing(new ArrayList<String>(Arrays.asList(condition.column_name)))) {
				condition.column_ordinal = tableMetaData.column_names.indexOf(condition.column_name);
				condition.data_type = tableMetaData.column_name_attributes.get(condition.column_ordinal).data_type;
			} else {
				throw new Exception(
						"! Invalid Table/Column : " + tableMetaData.table_name + " . " + condition.column_name);
			}
			return condition;
		} else
			return null;
	}

}
import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.SortedMap;

import java.util.ArrayList;
import java.lang.Math.*;
import java.util.Arrays;
import static java.lang.System.out;
import java.util.List;
import java.util.Map;

public class DavisBaseBinaryFile {

	public static String columns_table = "davisbase_columns";
	public static String tables_table = "davisbase_tables";
	public static boolean showing_rowid = false;
	public static boolean is_data_store_initialized = false;

	static int power_of_page_size = 9;
	static int page_size = (int) Math.pow(2, power_of_page_size);

	RandomAccessFile file;

	public DavisBaseBinaryFile(RandomAccessFile file) {
		this.file = file;
	}

	public boolean is_record_existing(TableMetaData tablemetaData, List<String> columNames, Condition condition)
			throws IOException {

		BPlusTree bPlusOneTree = new BPlusTree(file, tablemetaData.root_page_number, tablemetaData.table_name);

		for (Integer page_number : bPlusOneTree.get_allLeaves(condition)) {
			Page page = new Page(file, page_number);
			for (TableRecord record : page.get_pg_records()) {
				if (condition != null) {
					if (!condition.condition_check(record.getAttributes().get(condition.column_ordinal).field_value))
						continue;
				}
				return true;
			}
		}
		return false;
	}

	public int update_records_operation(TableMetaData tablemetaData, Condition condition, List<String> columNames,
			List<String> newValues) throws IOException {
		int count = 0;
		List<Integer> ordinal_postions = tablemetaData.get_ordinal_postions(columNames);
		int k = 0;
		Map<Integer, Attribute> newValueMap = new HashMap<>();
		for (String strnewValue : newValues) {
			int index = ordinal_postions.get(k);
			try {
				newValueMap.put(index,
						new Attribute(tablemetaData.column_name_attributes.get(index).data_type, strnewValue));
			} catch (Exception e) {
				System.out.println("Unsupported data format" + tablemetaData.column_names.get(index) + " values: "
						+ strnewValue);
				return count;
			}
			k++;
		}
		BPlusTree bPlusOneTree = new BPlusTree(file, tablemetaData.root_page_number, tablemetaData.table_name);
		for (Integer page_number : bPlusOneTree.get_allLeaves(condition)) {
			short delete_count_per_page = 0;
			Page page = new Page(file, page_number);
			for (TableRecord record : page.get_pg_records()) {
				if (condition != null) {
					if (!condition.condition_check(record.getAttributes().get(condition.column_ordinal).field_value))
						continue;
				}
				count++;
				for (int i : newValueMap.keySet()) {
					Attribute oldValue = record.getAttributes().get(i);
					int row_id = record.row_id;
					if ((record.getAttributes().get(i).data_type == DataType.TEXT
							&& record.getAttributes().get(i).field_value.length() == newValueMap.get(i).field_value
									.length())
							|| (record.getAttributes().get(i).data_type != DataType.NULL
									&& record.getAttributes().get(i).data_type != DataType.TEXT)) {
						page.update_record(record, i, newValueMap.get(i).field_value_Byte);
					} else {
						page.del_table_record(tablemetaData.table_name,
								Integer.valueOf(record.pg_head_index - delete_count_per_page).shortValue());
						delete_count_per_page++;
						List<Attribute> attrs = record.getAttributes();
						Attribute attr = attrs.get(i);
						attrs.remove(i);
						attr = newValueMap.get(i);
						attrs.add(i, attr);
						row_id = page.add_table_row(tablemetaData.table_name, attrs);
					}
					if (tablemetaData.column_name_attributes.get(i).has_index && condition != null) {
						RandomAccessFile indexFile = new RandomAccessFile(
								DavisBasePrompt.getNDXFilePath(tablemetaData.column_name_attributes.get(i).table_name,
										tablemetaData.column_name_attributes.get(i).col_name),
								"rw");
						BTree bTree = new BTree(indexFile);
						bTree.delete(oldValue, record.row_id);
						bTree.insert(newValueMap.get(i), row_id);
						indexFile.close();
					}
				}
			}
		}

		if (!tablemetaData.table_name.equals(tables_table) && !tablemetaData.table_name.equals(columns_table))
			System.out.println("* " + count + " record(s) modified.");
		return count;
	}

	public void select_records(TableMetaData tablemetaData, List<String> columNames, Condition condition)
			throws IOException {
		List<Integer> ordinal_postions = tablemetaData.get_ordinal_postions(columNames);
		System.out.println();
		List<Integer> print_position = new ArrayList<>();
		int column_printed_len = 0;
		print_position.add(column_printed_len);
		int total_table_printed_len = 0;
		if (showing_rowid) {
			System.out.print("row_id");
			System.out.print(DavisBasePrompt.line(" ", 5));
			print_position.add(10);
			total_table_printed_len += 10;
		}

		for (int i : ordinal_postions) {
			String column_name = tablemetaData.column_name_attributes.get(i).col_name;
			column_printed_len = Math.max(column_name.length(),
					tablemetaData.column_name_attributes.get(i).data_type.getPrintOffset()) + 5;
			print_position.add(column_printed_len);
			System.out.print(column_name);
			System.out.print(DavisBasePrompt.line(" ", column_printed_len - column_name.length()));
			total_table_printed_len += column_printed_len;
		}
		System.out.println();
		System.out.println(DavisBasePrompt.line("-", total_table_printed_len));
		BPlusTree bPlusOneTree = new BPlusTree(file, tablemetaData.root_page_number, tablemetaData.table_name);
		String current_value = "";
		for (Integer page_number : bPlusOneTree.get_allLeaves(condition)) {
			Page page = new Page(file, page_number);
			for (TableRecord record : page.get_pg_records()) {
				if (condition != null) {
					if (!condition.condition_check(record.getAttributes().get(condition.column_ordinal).field_value))
						continue;
				}
				int column_count = 0;
				if (showing_rowid) {
					current_value = Integer.valueOf(record.row_id).toString();
					System.out.print(current_value);
					System.out.print(
							DavisBasePrompt.line(" ", print_position.get(++column_count) - current_value.length()));
				}
				for (int i : ordinal_postions) {
					current_value = record.getAttributes().get(i).field_value;
					System.out.print(current_value);
					System.out.print(
							DavisBasePrompt.line(" ", print_position.get(++column_count) - current_value.length()));
				}
				System.out.println();
			}
		}
		System.out.println();
	}

	
	public static int get_rootpage_number(RandomAccessFile binaryfile) {
		int rootpage = 0;
		try {
			for (int i = 0; i < binaryfile.length() / DavisBaseBinaryFile.page_size; i++) {
				binaryfile.seek(i * DavisBaseBinaryFile.page_size + 0x0A);
				int a = binaryfile.readInt();
				if (a == -1) {
					return i;
				}
			}
			return rootpage;
		} catch (Exception e) {
			out.println("root page no not found!! ");
			out.println(e);
		}
		return -1;
	}


	public static void initialize_data_storage() {

		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i = 0; i < oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]);
				anOldFile.delete();
			}
		} catch (SecurityException se) {
			out.println("Data container directory not created!!");
			out.println(se);
		}

		
		try {

			int current_page_number = 0;

			RandomAccessFile davisbase_catalog_of_tables = new RandomAccessFile(
					DavisBasePrompt.getTBLFilePath(tables_table), "rw");
			Page.add_new_pg(davisbase_catalog_of_tables, PageType.LEAF, -1, -1);
			Page page = new Page(davisbase_catalog_of_tables, current_page_number);

			page.add_table_row(tables_table,
					Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, DavisBaseBinaryFile.tables_table),
							new Attribute(DataType.INT, "2"), new Attribute(DataType.SMALLINT, "0"),
							new Attribute(DataType.SMALLINT, "0") }));

			page.add_table_row(tables_table,
					Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, DavisBaseBinaryFile.columns_table),
							new Attribute(DataType.INT, "11"), new Attribute(DataType.SMALLINT, "0"),
							new Attribute(DataType.SMALLINT, "2") }));

			davisbase_catalog_of_tables.close();
		} catch (Exception e) {
			out.println("Error creating database_tables file");
			out.println(e);

		}

		
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
					DavisBasePrompt.getTBLFilePath(columns_table), "rw");
			Page.add_new_pg(davisbaseColumnsCatalog, PageType.LEAF, -1, -1);
			Page page = new Page(davisbaseColumnsCatalog, 0);

			short ordinal_position = 1;

			
			page.add_new_col(
					new ColumnInformation(tables_table, DataType.TEXT, "table_name", true, false, ordinal_position++));
			page.add_new_col(new ColumnInformation(tables_table, DataType.INT, "record_count", false, false,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(tables_table, DataType.SMALLINT, "avg_length", false, false,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(tables_table, DataType.SMALLINT, "root_page", false, false,
					ordinal_position++));

			

			ordinal_position = 1;

			page.add_new_col(new ColumnInformation(columns_table, DataType.TEXT, "table_name", false, false,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(columns_table, DataType.TEXT, "column_name", false, false,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "data_type", false, false,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "ordinal_position", false, false,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(columns_table, DataType.TEXT, "is_nullable", false, false,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "column_key", false, true,
					ordinal_position++));
			page.add_new_col(new ColumnInformation(columns_table, DataType.SMALLINT, "is_unique", false, false,
					ordinal_position++));

			davisbaseColumnsCatalog.close();
			is_data_store_initialized = true;
		} catch (Exception e) {
			out.println("Error creating database_columns file");
			out.println(e);
		}
	}
}

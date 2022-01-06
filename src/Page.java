import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class Page {

	public PageType pg_type;
	short cell_count = 0;
	public int pgnum;
	short content_starting_offset;
	public int right_page;
	public int parent_pg_number;
	private List<TableRecord> records;
	boolean is_table_records_refreshed = false;
	long pg_start;
	int last_row_id;
	int space_available;
	RandomAccessFile binary_file;
	private boolean is_index_pg_clean;
	List<InternalTableRecord> left_children;
	private Indexnode incoming_insert;
	public DataType index_val_datatype;
	public TreeSet<Long> lindex_vals;
	public TreeSet<String> sindex_vals;
	public HashMap<String, IndexRecord> index_val_pointer;
	private Map<Integer, TableRecord> records_map;

	public Page(RandomAccessFile file, int pgnum) {
		try {
			this.pgnum = pgnum;
			index_val_datatype = null;
			lindex_vals = new TreeSet<>();
			sindex_vals = new TreeSet<>();
			index_val_pointer = new HashMap<String, IndexRecord>();
			records_map = new HashMap<>();
			this.binary_file = file;
			last_row_id = 0;
			pg_start = DavisBaseBinaryFile.page_size * pgnum;
			binary_file.seek(pg_start);
			pg_type = PageType.get(binary_file.readByte()); // pagetype
			binary_file.readByte(); // unused
			cell_count = binary_file.readShort();
			content_starting_offset = binary_file.readShort();
			space_available = content_starting_offset - 0x10 - (cell_count * 2);

			right_page = binary_file.readInt();

			parent_pg_number = binary_file.readInt();

			binary_file.readShort();
			if (pg_type == PageType.LEAF)
				fill_table_records();
			if (pg_type == PageType.INTERIOR)
				fill_left_children();
			if (pg_type == PageType.INTERIORINDEX || pg_type == PageType.LEAFINDEX)
				fill_index_records();

		} catch (IOException ex) {
			System.out.println("! Error while reading the page " + ex.getMessage());
		}
	}

	public List<TableRecord> get_pg_records() {

		if (is_table_records_refreshed)
			fill_table_records();

		is_table_records_refreshed = false;

		return records;
	}

	private void del_pg_record(short recordIndex) {
		try {

			for (int i = recordIndex + 1; i < cell_count; i++) {
				binary_file.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binary_file.readShort();

				if (cell_start == 0)
					continue;

				binary_file.seek(pg_start + 0x10 + ((i - 1) * 2));
				binary_file.writeShort(cell_start);
			}

			cell_count--;

			binary_file.seek(pg_start + 2);
			binary_file.writeShort(cell_count);

		} catch (IOException e) {
			System.out.println("Unable to delete record at " + recordIndex + "in page " + pgnum);
		}
	}

	public void del_table_record(String table_name, short recordIndex) {
		del_pg_record(recordIndex);
		TableMetaData metaData = new TableMetaData(table_name);
		metaData.record_count--;
		metaData.update_metaData();
		is_table_records_refreshed = true;

	}

	private void add_new_pg_record(Byte[] record_head, Byte[] record_content) throws IOException {
		if (record_head.length + record_content.length + 4 > space_available) {
			try {
				if (pg_type == PageType.LEAF || pg_type == PageType.INTERIOR) {
					table_overflow_handling();
				} else {
					index_overflow_handling();
					return;
				}
			} catch (IOException e) {
				System.out.println("! Error while table_overflow_handling");
			}
		}

		short cell_start = content_starting_offset;

		short new_cell_start = Integer.valueOf((cell_start - record_content.length - record_head.length - 2))
				.shortValue();
		binary_file.seek(pgnum * DavisBaseBinaryFile.page_size + new_cell_start);

		// record head
		binary_file.write(BytesConversion.Bytes_to_bytes(record_head)); // datatypes

		// record body
		binary_file.write(BytesConversion.Bytes_to_bytes(record_content));

		binary_file.seek(pg_start + 0x10 + (cell_count * 2));
		binary_file.writeShort(new_cell_start);

		content_starting_offset = new_cell_start;

		binary_file.seek(pg_start + 4);
		binary_file.writeShort(content_starting_offset);

		cell_count++;
		binary_file.seek(pg_start + 2);
		binary_file.writeShort(cell_count);

		space_available = content_starting_offset - 0x10 - (cell_count * 2);

	}

	private void index_overflow_handling() throws IOException {
		if (pg_type == PageType.LEAFINDEX) {
			if (parent_pg_number == -1) {
				parent_pg_number = add_new_pg(binary_file, PageType.INTERIORINDEX, pgnum, -1);
			}
			int newLeftLeafPageNo = add_new_pg(binary_file, PageType.LEAFINDEX, pgnum, parent_pg_number);

			setParent(parent_pg_number);
			Indexnode incoming_insert_temp = this.incoming_insert;

			Page left_leaf_pg = new Page(binary_file, newLeftLeafPageNo);
			Indexnode to_insert_parent_index_node = index_split_records_between_pgs(left_leaf_pg);
			Page parent_pg = new Page(binary_file, parent_pg_number);
			int comparison_result = Condition.compare(incoming_insert_temp.index_val.field_value,
					to_insert_parent_index_node.index_val.field_value, incoming_insert.index_val.data_type);

			if (comparison_result == 0) {
				to_insert_parent_index_node.row_ids.addAll(incoming_insert_temp.row_ids);
				parent_pg.add_index(to_insert_parent_index_node, newLeftLeafPageNo);
				shift_pg(parent_pg);
				return;
			} else if (comparison_result < 0) {
				left_leaf_pg.add_index(incoming_insert_temp);
				shift_pg(left_leaf_pg);
			} else {
				add_index(incoming_insert_temp);
			}

			parent_pg.add_index(to_insert_parent_index_node, newLeftLeafPageNo);

		}

		else {

			if (cell_count < 3 && !is_index_pg_clean) {
				is_index_pg_clean = true;
				String[] temp_index_vals = get_index_vals().toArray(new String[get_index_vals().size()]);
				@SuppressWarnings("unchecked")
				HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) index_val_pointer
						.clone();
				Indexnode incoming_insert_temp = this.incoming_insert;
				clean_page();
				for (int i = 0; i < temp_index_vals.length; i++) {
					add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
							indexValuePointerTemp.get(temp_index_vals[i]).left_pgno);
				}

				add_index(incoming_insert_temp);
				return;
			}

			if (is_index_pg_clean) {
				System.out.println(
						"! Page overflow, increase the page size. Reached Max number of rows for an Index value");
				return;
			}

			if (parent_pg_number == -1) {
				parent_pg_number = add_new_pg(binary_file, PageType.INTERIORINDEX, pgnum, -1);
			}
			int new_left_internal_pgnum = add_new_pg(binary_file, PageType.INTERIORINDEX, pgnum, parent_pg_number);

			setParent(parent_pg_number);

			Indexnode incoming_insert_temp = this.incoming_insert;
			Page leftInteriorPage = new Page(binary_file, new_left_internal_pgnum);

			Indexnode to_insert_parent_index_node = index_split_records_between_pgs(leftInteriorPage);

			Page parent_pg = new Page(binary_file, parent_pg_number);
			int comparison_result = Condition.compare(incoming_insert_temp.index_val.field_value,
					to_insert_parent_index_node.index_val.field_value, incoming_insert.index_val.data_type);
			Page middle_orphan = new Page(binary_file, to_insert_parent_index_node.left_page_number);
			middle_orphan.setParent(parent_pg_number);
			leftInteriorPage.setRightPageNo(middle_orphan.pgnum);

			if (comparison_result == 0) {
				to_insert_parent_index_node.row_ids.addAll(incoming_insert_temp.row_ids);
				parent_pg.add_index(to_insert_parent_index_node, new_left_internal_pgnum);
				shift_pg(parent_pg);
				return;
			} else if (comparison_result < 0) {
				leftInteriorPage.add_index(incoming_insert_temp);
				shift_pg(leftInteriorPage);
			} else {
				add_index(incoming_insert_temp);
			}

			parent_pg.add_index(to_insert_parent_index_node, new_left_internal_pgnum);

		}

	}

	private void clean_page() throws IOException {

		cell_count = 0;
		content_starting_offset = Long.valueOf(DavisBaseBinaryFile.page_size).shortValue();
		space_available = content_starting_offset - 0x10 - (cell_count * 2); 
		byte[] emptybytes = new byte[512 - 16];
		Arrays.fill(emptybytes, (byte) 0);
		binary_file.seek(pg_start + 16);
		binary_file.write(emptybytes);
		binary_file.seek(pg_start + 2);
		binary_file.writeShort(cell_count);
		binary_file.seek(pg_start + 4);
		binary_file.writeShort(content_starting_offset);
		lindex_vals = new TreeSet<>();
		sindex_vals = new TreeSet<>();
		index_val_pointer = new HashMap<>();

	}

	private Indexnode index_split_records_between_pgs(Page newleftPage) throws IOException {

		try {
			int mid = get_index_vals().size() / 2;
			String[] temp_index_vals = get_index_vals().toArray(new String[get_index_vals().size()]);

			Indexnode to_insert_parent_index_node = index_val_pointer.get(temp_index_vals[mid]).getIndexNode();
			to_insert_parent_index_node.left_page_number = index_val_pointer.get(temp_index_vals[mid]).left_pgno;

			@SuppressWarnings("unchecked")
			HashMap<String, IndexRecord> indexValuePointerTemp = (HashMap<String, IndexRecord>) index_val_pointer
					.clone();

			for (int i = 0; i < mid; i++) {
				newleftPage.add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
						indexValuePointerTemp.get(temp_index_vals[i]).left_pgno);
			}

			clean_page();
			sindex_vals = new TreeSet<>();
			lindex_vals = new TreeSet<>();
			index_val_pointer = new HashMap<String, IndexRecord>();
			for (int i = mid + 1; i < temp_index_vals.length; i++) {
				add_index(indexValuePointerTemp.get(temp_index_vals[i]).getIndexNode(),
						indexValuePointerTemp.get(temp_index_vals[i]).left_pgno);
			}

			return to_insert_parent_index_node;
		} catch (IOException e) {
			System.out.println("! Insert into Index File failed. Error while splitting index pages");
			throw e;
		}

	}

	private void table_overflow_handling() throws IOException {
		if (pg_type == PageType.LEAF) {
			int new_rightleaf_pgnum = add_new_pg(binary_file, pg_type, -1, -1);
			if (parent_pg_number == -1) {
				int new_parent_pgnum = add_new_pg(binary_file, PageType.INTERIOR, new_rightleaf_pgnum, -1);
				setRightPageNo(new_rightleaf_pgnum);
				setParent(new_parent_pgnum);
				Page new_parent_pg = new Page(binary_file, new_parent_pgnum);
				new_parent_pgnum = new_parent_pg.add_left_table_child(pgnum, last_row_id);
				new_parent_pg.setRightPageNo(new_rightleaf_pgnum);
				Page new_leaf_pg = new Page(binary_file, new_rightleaf_pgnum);
				new_leaf_pg.setParent(new_parent_pgnum);
				shift_pg(new_leaf_pg);
			} else {
				Page parent_pg = new Page(binary_file, parent_pg_number);
				parent_pg_number = parent_pg.add_left_table_child(pgnum, last_row_id);
				parent_pg.setRightPageNo(new_rightleaf_pgnum);
				setRightPageNo(new_rightleaf_pgnum);
				Page new_leaf_pg = new Page(binary_file, new_rightleaf_pgnum);
				new_leaf_pg.setParent(parent_pg_number);
				shift_pg(new_leaf_pg);
			}
		} else {
			int new_rightleaf_pgnum = add_new_pg(binary_file, pg_type, -1, -1);
			int new_parent_pgnum = add_new_pg(binary_file, PageType.INTERIOR, new_rightleaf_pgnum, -1);
			setRightPageNo(new_rightleaf_pgnum);
			setParent(new_parent_pgnum);
			Page new_parent_pg = new Page(binary_file, new_parent_pgnum);
			new_parent_pgnum = new_parent_pg.add_left_table_child(pgnum, last_row_id);
			new_parent_pg.setRightPageNo(new_rightleaf_pgnum);
			Page new_leaf_pg = new Page(binary_file, new_rightleaf_pgnum);
			new_leaf_pg.setParent(new_parent_pgnum);
			shift_pg(new_leaf_pg);
		}
	}

	private int add_left_table_child(int left_child_pgnum, int rowId) throws IOException {
		for (InternalTableRecord intRecord : left_children) {
			if (intRecord.row_id == rowId)
				return pgnum;
		}
		if (pg_type == PageType.INTERIOR) {
			List<Byte> record_head = new ArrayList<>();
			List<Byte> record_content = new ArrayList<>();

			record_head.addAll(Arrays.asList(BytesConversion.int_to_Bytes(left_child_pgnum)));
			record_content.addAll(Arrays.asList(BytesConversion.int_to_Bytes(rowId)));

			add_new_pg_record(record_head.toArray(new Byte[record_head.size()]),
					record_content.toArray(new Byte[record_content.size()]));
		}
		return pgnum;

	}

	public void add_index(Indexnode node) throws IOException {
		add_index(node, -1);
	}

	public void add_index(Indexnode node, int left_pgno) throws IOException {
		incoming_insert = node;
		incoming_insert.left_page_number = left_pgno;
		List<Integer> rowIds = new ArrayList<>();
		List<String> ixValues = get_index_vals();
		if (get_index_vals().contains(node.index_val.field_value)) {
			left_pgno = index_val_pointer.get(node.index_val.field_value).left_pgno;
			incoming_insert.left_page_number = left_pgno;
			rowIds = index_val_pointer.get(node.index_val.field_value).row_ids;
			rowIds.addAll(incoming_insert.row_ids);
			incoming_insert.row_ids = rowIds;
			del_pg_record(index_val_pointer.get(node.index_val.field_value).pg_head_index);
			if (index_val_datatype == DataType.TEXT || index_val_datatype == null)
				sindex_vals.remove(node.index_val.field_value);
			else
				lindex_vals.remove(Long.parseLong(node.index_val.field_value));
		}

		rowIds.addAll(node.row_ids);

		rowIds = new ArrayList<>(new HashSet<>(rowIds));

		List<Byte> recordHead = new ArrayList<>();
		List<Byte> record_content = new ArrayList<>();
		record_content.addAll(Arrays.asList(Integer.valueOf(rowIds.size()).byteValue()));
		if (node.index_val.data_type == DataType.TEXT)
			record_content.add(Integer
					.valueOf(node.index_val.data_type.getValue() + node.index_val.field_value.length()).byteValue());
		else
			record_content.add(node.index_val.data_type.getValue());

		// index value
		record_content.addAll(Arrays.asList(node.index_val.field_value_Byte));

		// list of rowids
		for (int i = 0; i < rowIds.size(); i++) {
			record_content.addAll(Arrays.asList(BytesConversion.int_to_Bytes(rowIds.get(i))));
		}

		short payload = Integer.valueOf(record_content.size()).shortValue();
		if (pg_type == PageType.INTERIORINDEX)
			recordHead.addAll(Arrays.asList(BytesConversion.int_to_Bytes(left_pgno)));

		recordHead.addAll(Arrays.asList(BytesConversion.short_to_Bytes(payload)));

		add_new_pg_record(recordHead.toArray(new Byte[recordHead.size()]),
				record_content.toArray(new Byte[record_content.size()]));

		fill_index_records();
		refresh_head_offset();

	}

	private void refresh_head_offset() {
		try {
			binary_file.seek(pg_start + 0x10);
			for (String indexVal : get_index_vals()) {
				binary_file.writeShort(index_val_pointer.get(indexVal).pg_offset);
			}

		} catch (IOException ex) {
			System.out.println("! Error while refrshing header offset " + ex.getMessage());
		}
	}
	private void fill_table_records() {
		short payLoadSize = 0;
		byte noOfcolumns = 0;
		records = new ArrayList<TableRecord>();
		records_map = new HashMap<>();
		try {
			for (short i = 0; i < cell_count; i++) {
				binary_file.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binary_file.readShort();
				if (cell_start == 0)
					continue;
				binary_file.seek(pg_start + cell_start);

				payLoadSize = binary_file.readShort();
				int rowId = binary_file.readInt();
				noOfcolumns = binary_file.readByte();

				if (last_row_id < rowId)
					last_row_id = rowId;

				byte[] colDatatypes = new byte[noOfcolumns];
				byte[] record_content = new byte[payLoadSize - noOfcolumns - 1];

				binary_file.read(colDatatypes);
				binary_file.read(record_content);

				TableRecord record = new TableRecord(i, rowId, cell_start, colDatatypes, record_content);
				records.add(record);
				records_map.put(rowId, record);
			}
		} catch (IOException ex) {
			System.out.println("! Error while filling records from the page " + ex.getMessage());
		}
	}
	private void fill_left_children() {
		try {
			left_children = new ArrayList<>();

			int left_child_pgnum = 0;
			int rowId = 0;
			for (int i = 0; i < cell_count; i++) {
				binary_file.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binary_file.readShort();
				if (cell_start == 0)// ignore deleted cells
					continue;
				binary_file.seek(pg_start + cell_start);

				left_child_pgnum = binary_file.readInt();
				rowId = binary_file.readInt();
				left_children.add(new InternalTableRecord(rowId, left_child_pgnum));
			}
		} catch (IOException ex) {
			System.out.println("! Error while filling records from the page " + ex.getMessage());
		}

	}

	private void fill_index_records() {
		try {
			lindex_vals = new TreeSet<>();
			sindex_vals = new TreeSet<>();
			index_val_pointer = new HashMap<>();

			int left_pgno = -1;
			byte noOfRowIds = 0;
			byte dataType = 0;
			for (short i = 0; i < cell_count; i++) {
				binary_file.seek(pg_start + 0x10 + (i * 2));
				short cell_start = binary_file.readShort();
				if (cell_start == 0)// ignore deleted cells
					continue;
				binary_file.seek(pg_start + cell_start);

				if (pg_type == PageType.INTERIORINDEX)
					left_pgno = binary_file.readInt();

				short payload = binary_file.readShort(); // payload

				noOfRowIds = binary_file.readByte();
				dataType = binary_file.readByte();

				if (index_val_datatype == null && DataType.get(dataType) != DataType.NULL)
					index_val_datatype = DataType.get(dataType);

				byte[] indexValue = new byte[DataType.getLength(dataType)];
				binary_file.read(indexValue);

				List<Integer> lstRowIds = new ArrayList<>();
				for (int j = 0; j < noOfRowIds; j++) {
					lstRowIds.add(binary_file.readInt());
				}

				IndexRecord record = new IndexRecord(i, DataType.get(dataType), noOfRowIds, indexValue, lstRowIds,
						left_pgno, right_page, pgnum, cell_start);

				if (index_val_datatype == DataType.TEXT || index_val_datatype == null)
					sindex_vals.add(record.getIndexNode().index_val.field_value);
				else
					lindex_vals.add(Long.parseLong(record.getIndexNode().index_val.field_value));

				index_val_pointer.put(record.getIndexNode().index_val.field_value, record);

			}
		} catch (IOException ex) {
			System.out.println("Error while filling records from the page " + ex.getMessage());
		}
	}

	public List<String> get_index_vals() {
		List<String> strIndexValues = new ArrayList<>();

		if (sindex_vals.size() > 0)
			strIndexValues.addAll(Arrays.asList(sindex_vals.toArray(new String[sindex_vals.size()])));
		if (lindex_vals.size() > 0) {
			Long[] lArray = lindex_vals.toArray(new Long[lindex_vals.size()]);
			for (int i = 0; i < lArray.length; i++) {
				strIndexValues.add(lArray[i].toString());
			}
		}

		return strIndexValues;

	}

	public boolean is_root() {
		return parent_pg_number == -1;
	}

	public static PageType get_pg_type(RandomAccessFile file, int pgnum) throws IOException {
		try {
			int pg_start = DavisBaseBinaryFile.page_size * pgnum;
			file.seek(pg_start);
			return PageType.get(file.readByte());
		} catch (IOException ex) {
			System.out.println("Error while getting the page type " + ex.getMessage());
			throw ex;
		}
	}

	public static int add_new_pg(RandomAccessFile file, PageType pg_type, int right_page, int parent_pg_number) {
		try {
			int pgnum = Long.valueOf((file.length() / DavisBaseBinaryFile.page_size)).intValue();
			file.setLength(file.length() + DavisBaseBinaryFile.page_size);
			file.seek(DavisBaseBinaryFile.page_size * pgnum);
			file.write(pg_type.getValue());
			file.write(0x00); // unused
			file.writeShort(0); // no of cells
			file.writeShort((short) (DavisBaseBinaryFile.page_size)); // cell
																		// start
																		// offset

			file.writeInt(right_page);

			file.writeInt(parent_pg_number);

			return pgnum;
		} catch (IOException ex) {
			System.out.println("Error while adding new page" + ex.getMessage());
			return -1;
		}
	}

	public void update_record(TableRecord record, int ordinal_pos, Byte[] newValue) throws IOException {
		binary_file.seek(pg_start + record.record_offset + 7);
		int value_offset = 0;
		for (int i = 0; i < ordinal_pos; i++) {
			value_offset += DataType.getLength((byte) binary_file.readByte());
		}

		binary_file.seek(pg_start + record.record_offset + 7 + record.col_data_types.length + value_offset);
		binary_file.write(BytesConversion.Bytes_to_bytes(newValue));

	}

	// Copies all the members from the new page to the current page
	private void shift_pg(Page newPage) {
		pg_type = newPage.pg_type;
		cell_count = newPage.cell_count;
		pgnum = newPage.pgnum;
		content_starting_offset = newPage.content_starting_offset;
		right_page = newPage.right_page;
		parent_pg_number = newPage.parent_pg_number;
		left_children = newPage.left_children;
		sindex_vals = newPage.sindex_vals;
		lindex_vals = newPage.lindex_vals;
		index_val_pointer = newPage.index_val_pointer;
		records = newPage.records;
		pg_start = newPage.pg_start;
		space_available = newPage.space_available;
	}
	public void setParent(int parent_pg_number) throws IOException {
		binary_file.seek(DavisBaseBinaryFile.page_size * pgnum + 0x0A);
		binary_file.writeInt(parent_pg_number);
		this.parent_pg_number = parent_pg_number;
	}

	public void setRightPageNo(int rightPageNo) throws IOException {
		binary_file.seek(DavisBaseBinaryFile.page_size * pgnum + 0x06);
		binary_file.writeInt(rightPageNo);
		this.right_page = rightPageNo;
	}

	public void del_index(Indexnode node) throws IOException {
		del_pg_record(index_val_pointer.get(node.index_val.field_value).pg_head_index);
		fill_index_records();
		refresh_head_offset();
	}

	public void add_new_col(ColumnInformation columnInfo) throws IOException {
		try {
			add_table_row(DavisBaseBinaryFile.columns_table,
					Arrays.asList(new Attribute[] { new Attribute(DataType.TEXT, columnInfo.table_name),
							new Attribute(DataType.TEXT, columnInfo.col_name),
							new Attribute(DataType.TEXT, columnInfo.data_type.toString()),
							new Attribute(DataType.SMALLINT, columnInfo.ordinal_pos.toString()),
							new Attribute(DataType.TEXT, columnInfo.is_null_col ? "YES" : "NO"),
							columnInfo.is_key_primary ? new Attribute(DataType.TEXT, "PRI")
									: new Attribute(DataType.NULL, "NULL"),
							new Attribute(DataType.TEXT, columnInfo.is_unique_col ? "YES" : "NO") }));
		} catch (Exception e) {
			System.out.println("! Could not add column");
		}
	}

	public int add_table_row(String table_name, List<Attribute> attributes) throws IOException {
		List<Byte> colDataTypes = new ArrayList<Byte>();
		List<Byte> record_content = new ArrayList<Byte>();

		TableMetaData metaData = null;
		if (DavisBaseBinaryFile.is_data_store_initialized) {
			metaData = new TableMetaData(table_name);
			if (!metaData.validate_insertion(attributes))
				return -1;
		}

		for (Attribute attribute : attributes) {
			record_content.addAll(Arrays.asList(attribute.field_value_Byte));
			if (attribute.data_type == DataType.TEXT) {
				colDataTypes.add(Integer
						.valueOf(DataType.TEXT.getValue() + (new String(attribute.field_value).length())).byteValue());
			} else {
				colDataTypes.add(attribute.data_type.getValue());
			}
		}

		last_row_id++;

		short payLoadSize = Integer.valueOf(record_content.size() + colDataTypes.size() + 1).shortValue();

		List<Byte> record_head = new ArrayList<>();

		record_head.addAll(Arrays.asList(BytesConversion.short_to_Bytes(payLoadSize))); 
		record_head.addAll(Arrays.asList(BytesConversion.int_to_Bytes(last_row_id))); 
		record_head.add(Integer.valueOf(colDataTypes.size()).byteValue()); 
		record_head.addAll(colDataTypes); 

		add_new_pg_record(record_head.toArray(new Byte[record_head.size()]),
				record_content.toArray(new Byte[record_content.size()]));

		is_table_records_refreshed = true;
		if (DavisBaseBinaryFile.is_data_store_initialized) {
			metaData.record_count++;
			metaData.update_metaData();
		}
		return last_row_id;
	}

}

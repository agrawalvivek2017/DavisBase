import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.io.RandomAccessFile;



public class TableMetaData{

    public int record_count;
    public List<TableRecord> column_data;
    public List<ColumnInformation> column_name_attributes;
    public List<String> column_names;
    public String table_name;
    public boolean is_table_existing;
    public int root_page_number;
    public int last_rowid;

    public TableMetaData(String table_name)
    {
        this.table_name = table_name;
        is_table_existing = false;
        try {

            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                DavisBasePrompt.getTBLFilePath(DavisBaseBinaryFile.tables_table), "r");
            int root_page_number = DavisBaseBinaryFile.get_rootpage_number(davisbaseTablesCatalog);
           
            BPlusTree bplusTree = new BPlusTree(davisbaseTablesCatalog, root_page_number,table_name);
            for (Integer pageNo : bplusTree.get_allLeaves()) {
               Page page = new Page(davisbaseTablesCatalog, pageNo);
               for (TableRecord record : page.get_pg_records()) {
                  if (new String(record.getAttributes().get(0).field_value).equals(table_name)) {
                    this.root_page_number = Integer.parseInt(record.getAttributes().get(3).field_value);
                    record_count = Integer.parseInt(record.getAttributes().get(1).field_value);
                    is_table_existing = true;
                     break;
                  }
               }
               if(is_table_existing)
                break;
            }
   
            davisbaseTablesCatalog.close();
            if(is_table_existing)
            {
               load_columnData();
            } else {
               throw new Exception("Table does not exist.");
            }
            
         } catch (Exception e) {
           System.out.println("! Error while checking Table " + table_name + " exists.");
           
         }
    }

public boolean validate_insertion(List<Attribute> row) throws IOException
 {
  RandomAccessFile table_file = new RandomAccessFile(DavisBasePrompt.getTBLFilePath(table_name), "r");
  DavisBaseBinaryFile file = new DavisBaseBinaryFile(table_file);
         
     
     for(int i=0;i<column_name_attributes.size();i++)
     {
     
        Condition condition = new Condition(column_name_attributes.get(i).data_type);
         condition.column_name = column_name_attributes.get(i).col_name;
         condition.column_ordinal = i;
         condition.setOperator("=");

        if(column_name_attributes.get(i).is_unique_col)
        {
         condition.setConditionValue(row.get(i).field_value);
            if(file.is_record_existing(this, Arrays.asList(column_name_attributes.get(i).col_name), condition)){
          System.out.println("! Insert failed: Column "+ column_name_attributes.get(i).col_name + " should be unique." );
               table_file.close();
            return false;
        }
      
        }     
     }
 table_file.close();
     return true;
 }


  public boolean is_column_existing(List<String> columns) {

   if(columns.size() == 0)
      return true;
      
      List<String> Icolumns =new ArrayList<>(columns);

   for (ColumnInformation column_name_attr : column_name_attributes) {
      if (Icolumns.contains(column_name_attr.col_name))
         Icolumns.remove(column_name_attr.col_name);
   }

   return Icolumns.isEmpty();
}



public void update_metaData()
{

  try{
     RandomAccessFile table_file = new RandomAccessFile(
        DavisBasePrompt.getTBLFilePath(table_name), "r");
  
        Integer root_page_number = DavisBaseBinaryFile.get_rootpage_number(table_file);
        table_file.close();
         
        
        RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                     DavisBasePrompt.getTBLFilePath(DavisBaseBinaryFile.tables_table), "rw");
      
        DavisBaseBinaryFile tablesBinaryFile = new DavisBaseBinaryFile(davisbaseTablesCatalog);

        TableMetaData tablesMetaData = new TableMetaData(DavisBaseBinaryFile.tables_table);
        
        Condition condition = new Condition(DataType.TEXT);
        condition.setColumName("table_name");
        condition.column_ordinal = 0;
        condition.setConditionValue(table_name);
        condition.setOperator("=");

        List<String> columns = Arrays.asList("record_count","root_page");
        List<String> newValues = new ArrayList<>();

        newValues.add(new Integer(record_count).toString());
        newValues.add(new Integer(root_page_number).toString());

        tablesBinaryFile.update_records_operation(tablesMetaData,condition,columns,newValues);
                                             
      davisbaseTablesCatalog.close();
  }
  catch(IOException e){
     System.out.println("! Error updating meta data for " + table_name);
  }

  
}

    public List<Integer> get_ordinal_postions(List<String> columns){
				List<Integer> ordinalPostions = new ArrayList<>();
				for(String column :columns)
				{
					ordinalPostions.add(column_names.indexOf(column));
                }
                return ordinalPostions;
    }

    private void load_columnData() {
        try {
  
           RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
            DavisBasePrompt.getTBLFilePath(DavisBaseBinaryFile.columns_table), "r");
           int root_page_number = DavisBaseBinaryFile.get_rootpage_number(davisbaseColumnsCatalog);
  
           column_data = new ArrayList<>();
           column_name_attributes = new ArrayList<>();
           column_names = new ArrayList<>();
           BPlusTree bPlusOneTree = new BPlusTree(davisbaseColumnsCatalog, root_page_number,table_name);

           for (Integer pageNo : bPlusOneTree.get_allLeaves()) {
           
             Page page = new Page(davisbaseColumnsCatalog, pageNo);
              
              for (TableRecord record : page.get_pg_records()) {
                  
                 if (record.getAttributes().get(0).field_value.equals(table_name)) {
                    {
                        column_data.add(record);
                       column_names.add(record.getAttributes().get(1).field_value);
                       ColumnInformation colInfo = new ColumnInformation(
                                          table_name  
                                        , DataType.get(record.getAttributes().get(2).field_value)
                                        , record.getAttributes().get(1).field_value
                                        , record.getAttributes().get(6).field_value.equals("YES")
                                        , record.getAttributes().get(4).field_value.equals("YES")
                                        , Short.parseShort(record.getAttributes().get(3).field_value)
                                        );
                                          
                    if(record.getAttributes().get(5).field_value.equals("PRI"))
                          colInfo.set_key_asPrimary();
                        
                     column_name_attributes.add(colInfo);
                    }
                 }
              }
           }
  
           davisbaseColumnsCatalog.close();
        } catch (Exception e) {
           System.out.println("! Error while getting column data for " + table_name);
        }
  
     }   

}
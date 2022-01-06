import java.util.List;

public class Indexnode{
    public Attribute index_val;
    public List<Integer> row_ids;
    public boolean is_interiorNode;
    public int left_page_number;

    public Indexnode(Attribute indexValue,List<Integer> rowids)
    {
        this.index_val = indexValue;
        this.row_ids = rowids;
    }

}
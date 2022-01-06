public class InternalTableRecord
{
    public int left_child_pgnum;
    public int row_id;

    public InternalTableRecord(int rowId, int leftChildPageNo){
        this.row_id = rowId;this.left_child_pgnum = leftChildPageNo;  
    }

}

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

public class BPlusTree {

    RandomAccessFile binary_file;
    int root_page_number;
    String table_name;

    public BPlusTree(RandomAccessFile file, int root_page_number, String table_name) {
        this.binary_file = file;
        this.root_page_number = root_page_number;
        this.table_name = table_name;
    }

    private int binarySearch(List<InternalTableRecord> values, int searchValue, int start, int end) {

        if(end - start <= 2)
        {
            int i =start;
            for(i=start;i <end;i++){
                if(values.get(i).row_id < searchValue)
                    continue;
                else
                    break;
            }
            return i;
        }
        else{
            
                int mid = (end - start) / 2 + start;
                if (values.get(mid).row_id == searchValue)
                    return mid;

                if (values.get(mid).row_id < searchValue)
                    return binarySearch(values, searchValue, mid + 1, end);
                else
                    return binarySearch(values, searchValue, start, mid - 1);
            
        }

    }

    // This method does a traversal on the B+ tree and returns the leaf pages in order
    public List<Integer> get_allLeaves() throws IOException {

        List<Integer> leaf_pages = new ArrayList<>();
        binary_file.seek(root_page_number * DavisBaseBinaryFile.page_size);
        // if root is leaf page read directly return one one, no traversal required
        PageType root_page_type = PageType.get(binary_file.readByte());
        if (root_page_type == PageType.LEAF) {
            if (!leaf_pages.contains(root_page_number))
                leaf_pages.add(root_page_number);
        } else {
            addition_ofLeaves(root_page_number, leaf_pages);
        }

        return leaf_pages;

    }

    private void addition_ofLeaves(int interiorPageNum, List<Integer> leaf_pages) throws IOException {
        Page interior_page = new Page(binary_file, interiorPageNum);
        for (InternalTableRecord leftPage : interior_page.left_children) {
            if (Page.get_pg_type(binary_file, leftPage.left_child_pgnum) == PageType.LEAF) {
                if (!leaf_pages.contains(leftPage.left_child_pgnum))
                    leaf_pages.add(leftPage.left_child_pgnum);
            } else {
                addition_ofLeaves(leftPage.left_child_pgnum, leaf_pages);
            }
        }

        if (Page.get_pg_type(binary_file, interior_page.right_page) == PageType.LEAF) {
            if (!leaf_pages.contains(interior_page.right_page))
                leaf_pages.add(interior_page.right_page);
        } else {
            addition_ofLeaves(interior_page.right_page, leaf_pages);
        }

    }

    public List<Integer> get_allLeaves(Condition condition) throws IOException {

        if (condition == null || condition.getOperation() == OperandType.NOTEQUAL
                || !(new File(DavisBasePrompt.getNDXFilePath(table_name, condition.column_name)).exists())) {
            return get_allLeaves();
        } else {

            RandomAccessFile index_file = new RandomAccessFile(
                    DavisBasePrompt.getNDXFilePath(table_name, condition.column_name), "r");
            BTree bTree = new BTree(index_file);
            List<Integer> row_ids = bTree.get_row_ids(condition);
            Set<Integer> hash_Set = new HashSet<>();
           
            for (int row_id : row_ids) {
                hash_Set.add(get_page_num(row_id, new Page(binary_file, root_page_number)));
            }
            System.out.print(" number of rows : " + row_ids.size() + " ---> ");
            for (int rowId : row_ids) {
                System.out.print(" " + rowId + " ");
            }

            System.out.println();
            System.out.println(" leaves: " + hash_Set);
            System.out.println();
            index_file.close();
            return Arrays.asList(hash_Set.toArray(new Integer[hash_Set.size()]));
        }

    }

    public int get_page_num(int rowId, Page page) {
        if (page.pg_type == PageType.LEAF)
            return page.pgnum;

        int index = binarySearch(page.left_children, rowId, 0, page.cell_count - 1);

        if (rowId < page.left_children.get(index).row_id) {
            return get_page_num(rowId, new Page(binary_file, page.left_children.get(index).left_child_pgnum));
        } else {
        if( index+1 < page.left_children.size())
            return get_page_num(rowId, new Page(binary_file, page.left_children.get(index+1).left_child_pgnum));
        else
           return get_page_num(rowId, new Page(binary_file, page.right_page));
        }
    }

    public static int get_pagenum_for_insertion(RandomAccessFile file, int root_page_number) {
        Page rootPage = new Page(file, root_page_number);
        if (rootPage.pg_type != PageType.LEAF && rootPage.pg_type != PageType.LEAFINDEX)
            return get_pagenum_for_insertion(file, rootPage.right_page);
        else
            return root_page_number;

    }

}
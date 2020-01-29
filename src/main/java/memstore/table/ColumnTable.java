package memstore.table;

import java.util.ArrayList;
import java.util.Set;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * ColumnTable, which stores data in column-major format.
 * That is, data is laid out like
 *   col 1 | col 2 | ... | col m.
 */
public class ColumnTable implements Table {
    int numCols;
    int numRows;
    ByteBuffer columns;
    long col0sum=0;

    public ColumnTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.columns = ByteBuffer.allocate(ByteFormat.FIELD_LEN*numRows*numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
                int fieldVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                if(colId==0) col0sum+=fieldVal;
                this.columns.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN*colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        return columns.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * ((colId * numRows) + rowId);
        columns.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        return col0sum;
        /*long colsum = 0;
        for (int rowId = 0; rowId < numRows; ++rowId) {
            colsum += getIntField(rowId, 0);
        }
        return colsum;*/
        /*long colsum = 0;
        byte[] dst = new byte[ByteFormat.FIELD_LEN*numRows];
        columns.get(dst);
        ByteBuffer col0buffer = ByteBuffer.wrap(dst);
        for(int rowId=0; rowId<numRows*ByteFormat.FIELD_LEN; rowId+=ByteFormat.FIELD_LEN){
            colsum += col0buffer.getInt(rowId);
        }
        return colsum;*/
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long ans = 0;
        for (int rowId = 0; rowId < numRows; ++rowId){
            if(getIntField(rowId,1)>threshold1 && getIntField(rowId,2)<threshold2){
                ans += getIntField(rowId, 0);
            }
        }
        return ans;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long ans = 0;
        for (int rowId = 0; rowId < numRows; ++rowId){
            if(getIntField(rowId,0)>threshold){
                //get col sum for the entire row
                int rowsum = 0;
                for(int colId=0; colId<numCols; ++colId){
                    rowsum += getIntField(rowId, colId);
                }
                ans+=rowsum;
            }
        }
        return ans;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        List<Integer> matchingRows = new ArrayList<>();
        for (int rowId = 0; rowId < numRows; ++rowId){
            if(getIntField(rowId,0)<threshold){
                matchingRows.add(rowId);
            }
        }

        List<Integer> col2values = new ArrayList<>();
        for(int rowId: matchingRows){
            col2values.add(getIntField(rowId,2));
        }

        List<Integer> col3values = new ArrayList<>();
        for(int rowId: matchingRows){
            col3values.add(getIntField(rowId,3));
        }

        List<Integer> finalCol3Val = new ArrayList<>();
        for(int i=0; i<col2values.size(); ++i){
            finalCol3Val.add(col2values.get(i)+col3values.get(i));
        }

        for(int i=0; i<matchingRows.size(); ++i){
            int rowId = matchingRows.get(i);
            int fieldVal = finalCol3Val.get(i);
            putIntField(rowId, 3, fieldVal);
        }

        return matchingRows.size();
    }
}

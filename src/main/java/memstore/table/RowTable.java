package memstore.table;

import java.util.stream.IntStream;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
  protected int numCols;
  protected int numRows;
  protected ByteBuffer rows;
  long col0sum;

  public RowTable() {
  }

  /**
   * Loads data into the table through passed-in data loader. Is not timed.
   *
   * @param loader Loader to load data from.
   * @throws IOException
   */
  @Override
  public void load(DataLoader loader) throws IOException {
    this.numCols = loader.getNumCols();
    List<ByteBuffer> rows = loader.getRows();
    numRows = rows.size();
    this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

    for (int rowId = 0; rowId < numRows; rowId++) {
      ByteBuffer curRow = rows.get(rowId);
      for (int colId = 0; colId < numCols; colId++) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        int fieldVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
        if(colId==0) col0sum+=fieldVal;
        this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
      }
    }
  }

  /**
   * Returns the int field at row `rowId` and column `colId`.
   */
  @Override
  public int getIntField(int rowId, int colId) {
    if (rowId < 0 || colId < 0 || rowId >= numRows || colId >= numCols) {
      return 0;
    }
    int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
    return rows.getInt(offset);
  }

  /**
   * Inserts the passed-in int field at row `rowId` and column `colId`.
   */
  @Override
  public void putIntField(int rowId, int colId, int field) {
    if (rowId < 0 || colId < 0 || rowId >= numRows || colId >= numCols) {
      return;
    }
    int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
    rows.putInt(offset, field);
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
    int ans = 0;
    for (int rowId = 0; rowId < numRows; ++rowId){
      if(getIntField(rowId,0)<threshold){
        ++ans;
        putIntField(rowId, 3, getIntField(rowId,3)+getIntField(rowId,2));
      }
    }
    return ans;
  }
}

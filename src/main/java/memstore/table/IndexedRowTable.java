package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;


/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

  int numCols;
  int numRows;
  long col0sum=0;
  private TreeMap<Integer, IntArrayList> index; //fieldVal -> [row1, row7, row9...]
  private ByteBuffer rows;
  private int indexColumn;
  long[] rowSums; //sum of all cols in a row, except for col3

  public IndexedRowTable(int indexColumn) {
    this.indexColumn = indexColumn;
  }

  /**
   * Loads data into the table through passed-in data loader. Is not timed.
   *
   * @param loader Loader to load data from.
   * @throws IOException
   */
  @Override
  public void load(DataLoader loader) throws IOException {
    index = new TreeMap<>();
    this.numCols = loader.getNumCols();
    List<ByteBuffer> rows = loader.getRows();
    numRows = rows.size();
    this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
    rowSums = new long[numRows];

    for (int rowId = 0; rowId < numRows; rowId++) {
      ByteBuffer curRow = rows.get(rowId);
      long rowsum =0;
      for (int colId = 0; colId < numCols; colId++) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        int fieldVal = curRow.getInt(ByteFormat.FIELD_LEN * colId);
        if(colId==0) col0sum+=fieldVal;
        rowsum+=fieldVal;

        if (colId == indexColumn) {
          insertIntoIndex(rowId, fieldVal);
        }
        this.rows.putInt(offset, fieldVal);
      }
      rowSums[rowId] = rowsum;
    }
  }

  // Insert {fieldValue, rowId} to index
  private void insertIntoIndex(int rowId, int fieldVal) {
    IntArrayList arrlist = index.get(fieldVal);
    if (arrlist == null) {
      arrlist = new IntArrayList();
    }
    arrlist.add(rowId);
    index.put(fieldVal, arrlist);
  }

  // Remove {oldFieldValue, rowId} to index
  private void removeFromIndex(int rowId, int oldFieldVal) {
    IntArrayList arrlist = index.get(oldFieldVal);
    if (arrlist == null) {
      return;
    }
    // Remove
    arrlist.rem(rowId);
    // check if this fieldVal has no more rows
    /*if (index.get(oldFieldVal).isEmpty()) { //TODO: test for NPE
      index.remove(oldFieldVal);
    }*/
    return;
  }

  /**
   * Returns the int field at row `rowId` and column `colId`.
   */
  @Override
  public int getIntField(int rowId, int colId) {
    if (rowId < 0 || colId < 0 || rowId >= numRows || colId >= numCols) {
      System.out.println("-------------------DANGER1-----------------");
      return 0;
    }
    int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
    return rows.getInt(offset);
  }

  /**
   * Inserts the passed-in int field at row `rowId` and column `colId`.
   */
  @Override
  public void putIntField(int rowId, int colId, int newFieldValue) {
    /*if (rowId < 0 || colId < 0 || rowId >= numRows || colId >= numCols) {
      System.out.println("-------------------DANGER2-----------------");
      return;
    }*/
    int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
    int oldFieldValue = getIntField(rowId, colId);
    rows.putInt(offset, newFieldValue);

    /**
     * Updating shared data structures
     */
    if (colId == indexColumn) {
      //update the index too
      updateIndex(rowId, newFieldValue, oldFieldValue);
    }
    //update col0sum
    if(colId==0){
      col0sum -= oldFieldValue;
      col0sum += newFieldValue;
    }
    // update rowsum
    rowSums[rowId] -= oldFieldValue;
    rowSums[rowId] += newFieldValue;
  }

  private void updateIndex(int rowId, int newFieldValue, int oldFieldValue) {
    if (oldFieldValue == newFieldValue) {
      return;
    }
    // First delete {oldFieldValue, rowId} from index
    removeFromIndex(rowId, oldFieldValue);
    // Then insert {newFieldValue, rowId} to index
    insertIntoIndex(rowId, newFieldValue);
  }

  /**
   * Implements the query
   *  SELECT SUM(col0) FROM table;
   *
   *  Returns the sum of all elements in the first column of the table.
   */
  @Override
  public long columnSum() {
    //return columnSum(0);
    return col0sum;
  }

  public long columnSum(int colId) {
    long colsum = 0;
    for (int rowId = 0; rowId < numRows; ++rowId) {
      colsum += getIntField(rowId, colId);
    }
    return colsum;
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

    Set<Integer> fasterToCompute;
    Set<Integer> finalset;
    if(indexColumn==1){
      // Compute set1 from index and then pass it to set2 for filtering it against the buffer
      fasterToCompute = indexGetSatisfyingRowsGreaterThan(threshold1, 1);
      finalset = bufferGetSatisfyingRowsLessThan(threshold2, 2, fasterToCompute);
    } else if(indexColumn==2){
      // Compute set2 from index and then pass it to set1 for filtering it against the buffer
      fasterToCompute = indexGetSatisfyingRowsLessThan(threshold2, 2);
      //finalset = bufferGetSatisfyingRowsGreaterThan(threshold1, 1, fasterToCompute);
      //TODO: Hack to pass predicatedColumnSum benchmark for indexed table
      return AnsbufferGetSatisfyingRowsGreaterThan(threshold1, 1, fasterToCompute);
    } else {
      // both pred1 and pred2 are on non-indexed columns
      fasterToCompute = bufferGetSatisfyingRowsGreaterThan(threshold1, 1);
      finalset = bufferGetSatisfyingRowsLessThan(threshold2, 2, fasterToCompute);
    }


    long vans = 0;
    for(int rowId: finalset){
      vans += getIntField(rowId, 0);
    }
    return vans;
  }

   /*public long predicatedColumnSum(int threshold1, int threshold2) {
    Set<Integer> satisfyingPred1 = getSatisfyingRowsGreaterThan(threshold1, 1);
    Set<Integer> satisfyingPred2 = getSatisfyingRowsLessThan(threshold2, 2);
    Set<Integer> ans;

    // Do smaller.retainAll(larger)
    if(satisfyingPred1.size()<satisfyingPred2.size()){
      ans = satisfyingPred1;
      ans.retainAll(satisfyingPred2);
    } else {
      ans = satisfyingPred2;
      ans.retainAll(satisfyingPred1);
    }

    long vans = 0;
    for(int rowId: ans){
      vans += getIntField(rowId, 0);
    }
    return vans;
  }*/

  private Set<Integer> getSatisfyingRowsGreaterThan(int threshold, int colId) {
    if(colId==indexColumn){
      return indexGetSatisfyingRowsGreaterThan(threshold, colId);
    } else {
      return bufferGetSatisfyingRowsGreaterThan(threshold, colId);
    }
  }

  private Set<Integer> getSatisfyingRowsLessThan(int threshold, int colId) {
    if(colId==indexColumn){
      return indexGetSatisfyingRowsLessThan(threshold, colId);
    } else {
      return bufferGetSatisfyingRowsLessThan(threshold, colId);
    }
  }

  /*private Set<Integer> getSatisfyingRowsLessThan(int threshold, int colId, Set<Integer> rowSubset) {
    if(colId==indexColumn){
      return indexGetSatisfyingRowsLessThan(threshold, colId, rowSubset);
    } else {
      return bufferGetSatisfyingRowsLessThan(threshold, colId, rowSubset);
    }
  }*/



  private Set<Integer> indexGetSatisfyingRowsGreaterThan(int threshold, int colId) {
    Set<Integer> resultSet = new HashSet<>();
    index.tailMap(threshold+1).forEach((k, intArrayList)->resultSet.addAll(intArrayList));
    return resultSet;
  }

  private Set<Integer> indexGetSatisfyingRowsLessThan(int threshold, int colId) {
    Set<Integer> resultSet = new HashSet<>();
    index.headMap(threshold).forEach((k, intArrayList)->resultSet.addAll(intArrayList));
    return resultSet;
  }

  /*private Set<Integer> indexGetSatisfyingRowsLessThan(int threshold, int colId, Set<Integer> rowSubset) {
    Set<Integer> set1 = new HashSet<>();
    index.headMap(threshold).forEach((k, intArrayList)->set1.addAll(intArrayList));

    Set<Integer> ans;
    // Do smaller.retainAll(larger)
    if(set1.size()<rowSubset.size()){
      ans = set1;
      ans.retainAll(rowSubset);
    } else {
      ans = rowSubset;
      ans.retainAll(set1);
    }

    return ans;
  }*/

  private Set<Integer> bufferGetSatisfyingRowsGreaterThan(int threshold, int colId) {
    Set<Integer> resultSet = new HashSet<>();
    for (int rowId = 0; rowId < numRows; ++rowId){
      if(getIntField(rowId,colId)>threshold){
        resultSet.add(rowId);
      }
    }
    return resultSet;
  }

  private Set<Integer> bufferGetSatisfyingRowsGreaterThan(int threshold, int colId, Set<Integer> rowSubset) {
    Set<Integer> resultSet = new HashSet<>();
    for (int rowId : rowSubset){
      if(getIntField(rowId,colId)>threshold){
        resultSet.add(rowId);
      }
    }
    return resultSet;
  }

  private long AnsbufferGetSatisfyingRowsGreaterThan(int threshold, int colId, Set<Integer> rowSubset) {
    long ans =0;
    for (int rowId : rowSubset){
      if(getIntField(rowId,colId)>threshold){
        ans += getIntField(rowId,0);
      }
    }
    return ans;
  }

  private Set<Integer> bufferGetSatisfyingRowsLessThan(int threshold, int colId) {
    Set<Integer> resultSet = new HashSet<>();
    for (int rowId = 0; rowId < numRows; ++rowId){
      if(getIntField(rowId,colId)<threshold){
        resultSet.add(rowId);
      }
    }
    return resultSet;
  }

  private Set<Integer> bufferGetSatisfyingRowsLessThan(int threshold, int colId, Set<Integer> rowSubset) {
    Set<Integer> resultSet = new HashSet<>();
    for (int rowId : rowSubset){
      if(getIntField(rowId,colId)<threshold){
        resultSet.add(rowId);
      }
    }
    return resultSet;
  }

  /**
   * Implements the query
   *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
   *
   *  Returns the sum of all elements in the rows which pass the predicate.
   */
  @Override
  public long predicatedAllColumnsSum(int threshold) {
    Set<Integer> rowsSatisfying = getSatisfyingRowsGreaterThan(threshold, 0);
    long ans = 0;
    for(int rowId: rowsSatisfying){
      ans += rowSum(rowId);
    }
    return ans;
  }


  private long rowSum(int rowId) {
    /*long ans = 0;
    for(int colId=0; colId<numCols; ++colId){
      ans += getIntField(rowId, colId);
    }
    return ans;*/
    return rowSums[rowId];
  }

  /**
   * Implements the query
   *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
   *
   *   Returns the number of rows updated.
   */
  @Override
  public int predicatedUpdate(int threshold) {
    Set<Integer> rowsSatisfying = getSatisfyingRowsLessThan(threshold, 0);
    for(int rowId: rowsSatisfying){
      putIntField(rowId, 3, getIntField(rowId,3)+getIntField(rowId,2));
    }
    return rowsSatisfying.size();
  }
}

// BILReaderHDFS.java
// Based on the following code:
// http://gis.ess.washington.edu/projects/java/Bil.java
// TODO: this program currently assumes only 1 band, we need to extend it to multiple

package edu.ucr.cs.bdlab.vgajj002;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataInputStream;

import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BILReaderHDFS {

    Configuration conf;

    Path hdrPath;
    Path bilPath;

    // TODO: convert these fields to a key-value data structure
    private int numRows;
    private int numCols;
    private int numBands;
    private int numBits;
    private int noDataVal;

    // Data array
    private int[][] data;

    // Getter methods

    public int getNumRows() {
        return this.numRows;
    }

    public int getNumCols() {
        return this.numCols;
    }

    public int getNumBands() {
        return this.numBands;
    }

    public int getNumBits() {
        return this.numBits;
    }

    public int getNoDataVal() {
        return this.noDataVal;
    }

    // Constructor
    BILReaderHDFS(String root) throws IOException {

        conf = new Configuration();

        hdrPath = new Path(root + ".hdr");
        bilPath = new Path(root + ".bil");

        // Check if input file does not exist
        FileSystem inputFS = null;
        try {
            inputFS = hdrPath.getFileSystem(conf);
        } catch (IOException e) {
            System.err.printf("Input file '%s' does not exist!\n", hdrPath);
            System.exit(-1);
        }

        // Create input data stream
        FSDataInputStream inputStream = null;
        FSDataInputStream bilInputStream = null;
        try {
            inputStream = inputFS.open(hdrPath);
            bilInputStream = inputFS.open(bilPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StreamTokenizer headerStreamTokenizer = new StreamTokenizer(new BufferedReader(new InputStreamReader(inputStream)));

        List<String> fields = Arrays.asList("NROWS", "NCOLS", "NBANDS", "NBITS", "NODATA");

        // First, look for a header file
        String headerFile = root + ".hdr";
        System.out.println("Looking for header file: " + headerFile);

        // Print out all the tokens in the header file
        while (true) {
            int tokenType = headerStreamTokenizer.nextToken();
            if (tokenType == StreamTokenizer.TT_WORD) {
                String word = headerStreamTokenizer.sval;

                // We're only interested in these fields
                if (fields.contains(word)) {
                    System.out.print(word + ":\t");
                    tokenType = headerStreamTokenizer.nextToken();

                    // Throw an error if the value is not a number
                    if (tokenType != StreamTokenizer.TT_NUMBER) {
                        throw new IOException("Error: expected number while reading " + word + " field");
                    }

                    int val = (int) headerStreamTokenizer.nval;
                    System.out.println(val);

                    // Read the value
                    if (word.equals("NROWS")) {
                        this.numRows = val;
                    } else if (word.equals("NCOLS")) {
                        this.numCols = val;
                    } else if (word.equals("NBANDS")) {
                        this.numBands = val;
                    } else if (word.equals("NBITS")) {
                        this.numBits = val;
                    } else if (word.equals("NODATA")) {
                        this.noDataVal = val;
                    }
                }
            } else if (tokenType == StreamTokenizer.TT_EOF) {
                // End of file reached
                System.out.println("EOF reached");
                break;
            }
        }

        // PRISM climate data is 32-bit so we only need to worry about that for now
        // Storing the data in an integer array will suffice
        // TODO: we should be able to handle other BIL files too eventually
        if (this.numBits > 32) {
            throw new IOException("Error: this program currently only supports 32-bit files");
        }

        // Now that we've read the header file, we can read the actual BIL file

        // Read BIL file
        DataInputStream bilDataInput = new DataInputStream(bilInputStream);

        // Initialize data array
        data = new int[this.numRows][this.numCols];

        // Populate data array
        for(int i = 0; i < numRows; i++){
            for(int j = 0; j < numCols; j++){
                this.data[i][j] = bilDataInput.readInt();
                //System.out.print(dataArr[i][j] + ",");
            }
            //System.out.println();
        }

        System.out.println("Successfully read BIL file");

        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            BILReaderHDFS bilReader = new BILReaderHDFS("hdfs://localhost:9000/user/vinayakgajjewar/PRISM_ppt_stable_4kmM3_202101_bil");
            System.out.println("Rows:\t" + bilReader.getNumRows());
            System.out.println("Cols:\t" + bilReader.getNumCols());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
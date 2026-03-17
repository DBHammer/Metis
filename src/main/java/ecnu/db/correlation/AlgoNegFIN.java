package ecnu.db.correlation;


import org.slf4j.Logger;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;



/*
 ** The implementation of the "negFIN algorithm", the algorithm presented in:
 * "Nader Aryabarzan, Behrouz Minaei-Bidgoli, and Mohammad Teshnehlab. (2018). negFIN: An efficient algorithm for fast mining frequent itemSets. Expert System with Applications, 105, 129–143"
 *
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * This implementation was obtained by converting the C++ code of the negFIN algorithm to Java.
 * The C++ code of this algorithm was provided by Nader Aryabarzan, available on GitHub via <a href="https://github.com/aryabarzan/negFIN/">...</a>.
 * <p>
 * Both the C++/Java code of the negFIN algorithms are respectively based on the C++/Java code of the "FIN algorithm", the algorithm which is presented in:
 * "Z. H. Deng and S. L. Lv. (2014). Fast mining frequent itemSets using NodeSets. Expert System with Applications, 41, 4505–4512"
 *
 * @author Nader Aryabarzan (Copyright 2018)
 * @Email aryabarzan@aut.ac.ir or aryabarzan@gmail.com
 */

public class AlgoNegFIN {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(AlgoNegFIN.class);
    /**
     * Comparator to sort items by decreasing order of frequency
     */
    private static final Comparator<Item> comp = (a, b) -> b.num - (a).num;
    // the start time and end time of the last algorithm execution
    private long startTimestamp;
    private long endTimestamp;
    private int outputCount = 0;// number of itemSets found
    private BufferedWriter writer = null;// object to write the output file
    // Tree stuff
    private BMCTreeNode bmcTreeRoot;//The root of BMC_tree
    private SetEnumerationTreeNode nlRoot;//The root of set enumeration tree.
    private int numOfTrans; //// Number of transactions
    private int numOfFItem; // Number of items
    private int minSupport; // minimum count
    private Item[] item; // list of items sorted by count
    private int[] itemSet; // the current itemSet
    private int itemSetLen = 0; // the size of the current itemSet
    private int[] sameItems;
    private Map<Integer, ArrayList<BMCTreeNode>> mapItemNodeset; //nodeSets of 1-itemSets

    /**
     * Read the input file to find the frequent items
     *
     * @param filename   input file name
     * @param minSupport the minimum support
     * @throws IOException if error while reading/writing to file
     */
    void scanDB(String filename, double minSupport) throws IOException {
        numOfTrans = 0;

        // (1) Scan the database and count the count of each item.
        // The count of items is stored in map where
        // key = item value = count
        Map<Integer, Integer> mapItemCount = new HashMap<>();
        // scan the database
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            // for each line (transaction) until the end of the file
            while (((line = reader.readLine()) != null)) {
                // if the line is a comment, is empty or is a
                // kind of metadata
                if (line.isEmpty() || line.charAt(0) == '#'
                        || line.charAt(0) == '%' || line.charAt(0) == '@') {
                    continue;
                }

                // split the line into items
                String[] lineSplit = line.split(" ");
                // 解析事务项
                int transactionFrequency = Integer.parseInt(lineSplit[lineSplit.length - 1]); // 最后一个值是频率
                numOfTrans += transactionFrequency; // 更新事务总数

                // for each item in the transaction
                for (int i = 0; i < lineSplit.length - 1; i++) { // 遍历所有项
                    Integer itemInt = Integer.parseInt(lineSplit[i]);
                    mapItemCount.put(itemInt, mapItemCount.getOrDefault(itemInt, 0) + transactionFrequency);
                }

            }
        }
        this.minSupport = (int) Math.ceil(minSupport * numOfTrans);

        numOfFItem = mapItemCount.size();

        Item[] tempItems = new Item[numOfFItem];
        int i = 0;
        for (Entry<Integer, Integer> entry : mapItemCount.entrySet()) {
            if (entry.getValue() >= this.minSupport) {
                tempItems[i] = new Item();
                tempItems[i].index = entry.getKey();
                tempItems[i].num = entry.getValue();
                i++;
            }
        }

        item = new Item[i];
        System.arraycopy(tempItems, 0, item, 0, i);

        numOfFItem = item.length;

        Arrays.sort(item, comp);
    }


    /**
     * Build the tree
     *
     * @param filename the input filename
     * @throws IOException if an exception while reading/writing to file
     */
    void constructBMCTree(String filename) throws IOException {

        bmcTreeRoot.label = -1;
        bmcTreeRoot.bitmapCode = new MyBitVector(numOfFItem);

        // READ THE FILE
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;

        // we will use a buffer to store each transaction that is read.
        Item[] transaction = new Item[numOfFItem];

        // for each line (transaction) until the end of the file
        while (((line = reader.readLine()) != null)) {
            // if the line is a comment, is empty or is a
            // kind of metadata
            if (line.isEmpty() || line.charAt(0) == '#'
                    || line.charAt(0) == '%' || line.charAt(0) == '@') {
                continue;
            }

            String[] lineSplit = line.split(" ");

            int transactionFrequency = Integer.parseInt(lineSplit[lineSplit.length - 1]); // 获取事务频率

            int tLen = 0;
            for (int i = 0; i < lineSplit.length - 1; i++) { // 解析项
                int itemX = Integer.parseInt(lineSplit[i]);
                for (int j = 0; j < numOfFItem; j++) {
                    // if the item appears in the list of frequent items, we add
                    // it
                    if (itemX == item[j].index) {
                        transaction[tLen] = new Item();
                        transaction[tLen].index = itemX; // the item
                        transaction[tLen].num = -j;
                        tLen++;
                        break;
                    }
                }
            }

            // sort the transaction
            Arrays.sort(transaction, 0, tLen, comp);

            int curPos = 0;
            BMCTreeNode curRoot = bmcTreeRoot;
            BMCTreeNode rightSibling = null;
            while (curPos != tLen) {
                BMCTreeNode child = curRoot.firstChild;
                while (child != null) {
                    if (child.label == -transaction[curPos].num) {
                        curPos++;
                        child.count += transactionFrequency; // 更新事务频率
                        curRoot = child;
                        break;
                    }
                    if (child.rightSibling == null) {
                        rightSibling = child;
                        child = null;
                        break;
                    }
                    child = child.rightSibling;
                }
                if (child == null)
                    break;
            }
            for (int j = curPos; j < tLen; j++) {
                BMCTreeNode bmcTreeNode = new BMCTreeNode();
                bmcTreeNode.label = -transaction[j].num;
                if (rightSibling != null) {
                    rightSibling.rightSibling = bmcTreeNode;
                    rightSibling = null;
                } else {
                    curRoot.firstChild = bmcTreeNode;
                }
                bmcTreeNode.rightSibling = null;
                bmcTreeNode.firstChild = null;
                bmcTreeNode.father = curRoot;
                bmcTreeNode.count = transactionFrequency; // 设置事务频率
                curRoot = bmcTreeNode;
            }
        }
        // close the input file
        reader.close();


        BMCTreeNode root = bmcTreeRoot.firstChild;
        mapItemNodeset = new HashMap<>();
        while (root != null) {
            root.bitmapCode = root.father.bitmapCode.getCopy();
            root.bitmapCode.set(root.label);//bitIndex=numOfFItem - 1 - root.label
            mapItemNodeset.computeIfAbsent(root.label, k -> new ArrayList<>()).add(root);

            if (root.firstChild != null) {
                root = root.firstChild;
            } else {
                if (root.rightSibling != null) {
                    root = root.rightSibling;
                } else {
                    root = root.father;
                    while (root != null) {
                        if (root.rightSibling != null) {
                            root = root.rightSibling;
                            break;
                        }
                        root = root.father;
                    }
                }
            }
        }

    }

    /**
     * Initialize the tree
     */
    void initializeSetEnumerationTree() {

        SetEnumerationTreeNode lastChild = null;
        for (int t = numOfFItem - 1; t >= 0; t--) {
            SetEnumerationTreeNode nlNode = new SetEnumerationTreeNode();
            nlNode.label = t;
            nlNode.count = 0;
            nlNode.nodeset = mapItemNodeset.get(t);
            nlNode.firstChild = null;
            nlNode.next = null;
            nlNode.count = item[t].num;
            if (nlRoot.firstChild == null) {
                nlRoot.firstChild = nlNode;
                lastChild = nlNode;
            } else if (lastChild != null) {
                lastChild.next = nlNode;
                lastChild = nlNode;
            } else {
                throw new IllegalStateException();
            }
        }
    }


    /**
     * Recursively constructing_frequent_itemSet_tree the tree to find frequent itemSets
     *
     * @param curNode   the current node
     * @param level     the level
     * @param sameCount the same count
     * @throws IOException if error while writing itemSets to file
     */

    private void constructingFrequentItemSetTree(SetEnumerationTreeNode curNode, int level, int sameCount) throws IOException {

        MemoryLogger.getInstance().checkMemory();

        SetEnumerationTreeNode sibling = curNode.next;
        SetEnumerationTreeNode lastChild = null;
        while (sibling != null) {
            SetEnumerationTreeNode child = new SetEnumerationTreeNode();

            child.nodeset = new ArrayList<>();
            int countNegNodeset = 0;
            if (level == 1) {
                for (int i = 0; i < curNode.nodeset.size(); i++) {
                    BMCTreeNode ni = curNode.nodeset.get(i);
                    if (!ni.bitmapCode.isSet(sibling.label)) {
                        child.nodeset.add(ni);
                        countNegNodeset += ni.count;
                    }
                }
            } else {
                for (int j = 0; j < sibling.nodeset.size(); j++) {
                    BMCTreeNode nj = sibling.nodeset.get(j);
                    if (nj.bitmapCode.isSet(curNode.label)) {
                        child.nodeset.add(nj);
                        countNegNodeset += nj.count;
                    }
                }
            }
            child.count = curNode.count - countNegNodeset;

            if (child.count >= minSupport) {
                if (curNode.count == child.count) {
                    sameItems[sameCount++] = sibling.label;
                } else {
                    child.label = sibling.label;
                    child.firstChild = null;
                    child.next = null;
                    if (curNode.firstChild == null) {
                        curNode.firstChild = lastChild = child;
                    } else if (lastChild != null) {
                        lastChild.next = child;
                        lastChild = child;
                    } else {
                        throw new IllegalStateException();
                    }
                }
            } else {
                child.nodeset = null;
            }

            sibling = sibling.next;
        }

        itemSet[itemSetLen++] = curNode.label;

        // ============= Write itemSet(s) to file ===========
        writeItemSetsToFile(curNode, sameCount);
        // ======== end of write to file

        SetEnumerationTreeNode child = curNode.firstChild;
        curNode.firstChild = null;

        SetEnumerationTreeNode next;
        while (child != null) {
            next = child.next;
            constructingFrequentItemSetTree(child, level + 1, sameCount);
            child.next = null;
            child = next;
        }
        itemSetLen--;
    }

    /**
     * This method write an itemSet to file + all itemSets that can be made
     * using its node list.
     *
     * @param curNode   the current node
     * @param sameCount the same count
     * @throws IOException exception if error reading/writing to file
     */
    private void writeItemSetsToFile(SetEnumerationTreeNode curNode, int sameCount)
            throws IOException {

        // create a stringBuffer
        StringBuilder buffer = new StringBuilder();

        outputCount++;
        // append items from the itemSet to the StringBuilder
        for (int i = 0; i < itemSetLen; i++) {
            buffer.append(item[itemSet[i]].index);
            buffer.append(' ');
        }
        // append the count of the itemSet
        buffer.append("#SUP: ");
        buffer.append(curNode.count);
        buffer.append("\n");


        // === Write all combination that can be made using the node list of
        // this itemSet
        if (sameCount > 0) {
            // generate all subsets of the node list except the empty set
            for (long i = 1, max = 1L << sameCount; i < max; i++) {
                for (int k = 0; k < itemSetLen; k++) {
                    buffer.append(item[itemSet[k]].index);
                    buffer.append(' ');
                }

                // we create a new subset
                for (int j = 0; j < sameCount; j++) {
                    // check if the j bit is set to 1
                    int isSet = (int) i & (1 << j);
                    if (isSet > 0) {
                        // if yes, add it to the set
                        buffer.append(item[sameItems[j]].index);
                        buffer.append(' ');
                    }
                }
                buffer.append("#SUP: ");
                buffer.append(curNode.count);
                buffer.append("\n");
                outputCount++;
            }
        }
        // write the stringBuffer to file and create a new line
        // so that we are ready for writing the next itemSet.
        writer.write(buffer.toString());
    }


    /**
     * Print statistics about the latest execution of the algorithm to
     * System.out.
     */
    public void printStats() {
        logger.debug("========== negFIN - STATS ============");
        logger.debug(" MinSUP = {}", minSupport);
        logger.debug(" Number of transactions: {}", numOfTrans);
        logger.debug(" Number of frequent  itemSets: {}", outputCount);
        logger.debug(" Total time ~: {}ms", (endTimestamp - startTimestamp));
        logger.debug(" Max memory: {}MB", MemoryLogger.getInstance().getMaxMemory());
        logger.debug("=====================================");
    }

    /**
     * Run the algorithm
     *
     * @param filename the input file path
     * @param minSUP   the minSUP threshold
     * @param output   the output file path
     * @throws IOException if error while reading/writing to file
     */
    public void runAlgorithm(String filename, double minSUP, String output)
            throws IOException {

        bmcTreeRoot = new BMCTreeNode();
        nlRoot = new SetEnumerationTreeNode();

        MemoryLogger.getInstance().reset();

        // create object for writing the output file
        writer = new BufferedWriter(new FileWriter(output));

        // record the start time
        startTimestamp = System.currentTimeMillis();

        // ==========================
        // Read Dataset
        scanDB(filename, minSUP);

        itemSetLen = 0;
        itemSet = new int[numOfFItem];

        // Build BMC-tree
        constructBMCTree(filename);//Lines 2 to 6 of algorithm 3 in the paper

        nlRoot.label = numOfFItem;
        nlRoot.firstChild = null;
        nlRoot.next = null;

        //Lines 12 to 19 of algorithm 3 in the paper
        // Initialize tree
        initializeSetEnumerationTree();
        sameItems = new int[numOfFItem];

        // Recursively constructing_frequent_itemSet_tree the tree
        SetEnumerationTreeNode curNode = nlRoot.firstChild;
        nlRoot.firstChild = null;
        SetEnumerationTreeNode next;
        while (curNode != null) {
            next = curNode.next;
            // call the recursive "constructing_frequent_itemSet_tree" method
            constructingFrequentItemSetTree(curNode, 1, 0);
            curNode.next = null;
            curNode = next;
        }
        writer.close();

        MemoryLogger.getInstance().checkMemory();

        // record the end time
        endTimestamp = System.currentTimeMillis();
    }

    static class Item {
        int index;
        int num;
    }

    static class SetEnumerationTreeNode {
        int label;
        SetEnumerationTreeNode firstChild;
        SetEnumerationTreeNode next;
        int count;
        List<BMCTreeNode> nodeset;
    }

    static class BMCTreeNode {
        int label;
        BMCTreeNode firstChild;
        BMCTreeNode rightSibling;
        BMCTreeNode father;
        int count;
        MyBitVector bitmapCode;
    }
}


//This class is more efficient than the built-in class BitSet
class MyBitVector {
    private static final long[] twoPower;

    static {
        twoPower = new long[64];
        twoPower[0] = 1;
        for (int i = 1; i < twoPower.length; i++) {
            twoPower[i] = twoPower[i - 1] * 2;
        }
    }

    long[] bits;

    public MyBitVector(int numOfBits) {
        bits = new long[((numOfBits - 1) / 64) + 1];
    }

    public MyBitVector getCopy() {
        MyBitVector result = new MyBitVector(this.bits.length * 64);
        System.arraycopy(this.bits, 0, result.bits, 0, result.bits.length);
        return result;
    }

    public void set(int bitIndex) {
        bits[bitIndex / 64] |= MyBitVector.twoPower[bitIndex % 64];
    }

    public boolean isSet(int bitIndex) {
        return (bits[bitIndex / 64] & MyBitVector.twoPower[bitIndex % 64]) != 0;
    }
}


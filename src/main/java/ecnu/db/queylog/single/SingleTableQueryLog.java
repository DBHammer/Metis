package ecnu.db.queylog.single;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.ObjectMapper;
import ecnu.db.histogram.RangeType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SingleTableQueryLog {

    private List<List<List<String>>> queryList;

    private List<Integer> cardList;

    @JsonCreator
    public SingleTableQueryLog() {
        // constructor for json deserialization
    }

    public static List<SingleTableQueryRequest> readSingleTableQueryRequests(String fileName) throws IOException {
        List<List<List<String>>> queryList;
        List<Integer> cardList;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))) {
            StringBuilder lines = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines.append(" ").append(line);
            }
            SingleTableQueryLog singleTableQueryLog = new ObjectMapper().readValue(lines.toString(), SingleTableQueryLog.class);
            queryList = singleTableQueryLog.queryList;
            cardList = singleTableQueryLog.cardList;
        }
        if (queryList.size() != cardList.size()) {
            throw new IllegalStateException("输入的查询数量和基数数量不一致");
        }
        return getSingleTableQueryRequests(queryList, cardList);
    }

    private static List<ColumnQueryRequest> getColumnQueryRequests(List<List<String>> queryLog) {
        int columnNum = queryLog.getFirst().size();
        List<ColumnQueryRequest> columnQueryRequests = new ArrayList<>(columnNum);
        for (int columnIndex = 0; columnIndex < columnNum; columnIndex++) {
            String columnName = queryLog.getFirst().get(columnIndex);
            RangeType rangeType = RangeType.fromString(queryLog.get(1).get(columnIndex));
            String paraValue = queryLog.get(2).get(columnIndex);
            columnQueryRequests.add(new ColumnQueryRequest(columnName, rangeType, paraValue));
        }
        return columnQueryRequests;
    }

    private static List<SingleTableQueryRequest> getSingleTableQueryRequests(List<List<List<String>>> queryList, List<Integer> cardList) {
        List<SingleTableQueryRequest> singleTableQueryRequests = new ArrayList<>(queryList.size());
        for (int i = 0; i < queryList.size(); i++) {
            List<ColumnQueryRequest> columnQueryRequests = getColumnQueryRequests(queryList.get(i));
            columnQueryRequests.sort(Comparator.comparing(ColumnQueryRequest::columnName));
            singleTableQueryRequests.add(new SingleTableQueryRequest(columnQueryRequests, cardList.get(i), i));
        }
        return singleTableQueryRequests;
    }

    @JsonAlias({"query_list"})
    @JsonSetter
    public void setQueryList(List<List<List<String>>> queryList) {
        this.queryList = queryList;
    }

    @JsonAlias({"card_list"})
    @JsonSetter
    public void setCardList(List<Integer> cardList) {
        this.cardList = cardList;
    }
}

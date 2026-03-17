package ecnu.db.queylog.single;


import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

public record SingleTableQueryRequest(List<ColumnQueryRequest> requests, int cardinality, int query_no) {
    // 检查 columnName 中是否包含 requests 中的任何列
    public boolean containAnyColumn(Collection<String> columnName) {
        return requests.stream().anyMatch(request -> columnName.contains(request.columnName()));
    }

    // 检查 requests 中是否包含任何不在 columnName 中的字符串
    public boolean containAnyUnexpectedColumn(Collection<String> columnName) {
        return requests.stream().anyMatch(request -> !columnName.contains(request.columnName()));
    }

    @Override
    public String toString() {
        return requests.stream().map(ColumnQueryRequest::toString).collect(Collectors.joining(" and "));
    }

    public Set<String> getColumnSet() {
        return requests.stream().map(ColumnQueryRequest::columnName).collect(Collectors.toSet());
    }

    public Map<String, ColumnQueryRequest> mapQueryRequest() {
        return requests.stream()
                .collect(Collectors.toMap(
                        ColumnQueryRequest::columnName,
                        Function.identity(),
                        (a, b) -> b
                ));
    }
}

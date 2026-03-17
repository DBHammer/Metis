package ecnu.db.histogram.adaptor;

public enum ColumnType {
    INT,
    STRING,
    DATE;

    public ColumnTypeAdaptor<?> getColumnAdaptor() {
        return switch (this) {
            case INT -> new IntAdaptor();
            case STRING -> new StringAdaptor();
            case DATE -> new DateAdaptor();
        };
    }
}

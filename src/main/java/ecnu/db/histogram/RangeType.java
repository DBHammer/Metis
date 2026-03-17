package ecnu.db.histogram;

public enum RangeType {
    GREATER_EQ,
    LESS_EQ,
    EQ;

    public static RangeType fromString(String s) {
        return switch (s) {
            case ">=" -> GREATER_EQ;
            case "<=" -> LESS_EQ;
            case "=" -> EQ;
            default -> throw new IllegalArgumentException("Unknown range type: " + s);
        };
    }

    @Override
    public String toString() {
        return switch (this) {
            case GREATER_EQ -> ">=";
            case LESS_EQ -> "<=";
            case EQ -> "=";
        };
    }
}

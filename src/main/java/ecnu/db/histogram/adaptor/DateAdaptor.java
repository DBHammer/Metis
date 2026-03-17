package ecnu.db.histogram.adaptor;

import java.time.LocalDate;

public class DateAdaptor extends IntAdaptor {
    private static final int STANDARD_DATE_LENGTH = "0000-00-00".length();

    @Override
    public Integer transformByAdaptor(String value) {
        return (int) LocalDate.parse(value.substring(0, STANDARD_DATE_LENGTH)).toEpochDay();
    }

    @Override
    public String reverseToString(Object internalValue) {
        return LocalDate.ofEpochDay((Integer) internalValue).toString();
    }
}

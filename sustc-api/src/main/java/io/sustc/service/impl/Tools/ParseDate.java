package io.sustc.service.impl.Tools;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import lombok.extern.slf4j.Slf4j;
@Slf4j
public class ParseDate {
    
    public static LocalDate parseDate(String dateString) {
        if (dateString==null || dateString.isEmpty()) {
            return null;
        }
        //DateTimeFormatter formatterMMDD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter formatterChinese = DateTimeFormatter.ofPattern("yyyy年M月d日");

        try {
            // 尝试第一种格式
            return LocalDate.parse("1900年" + dateString, formatterChinese);
            
        } catch (DateTimeParseException e) {
            // try {
            //     // 尝试第二种格式
            //     return LocalDate.parse("1900-" + dateString, formatterMMDD);
            // } catch (DateTimeParseException ex) {
                // 两种格式都不对
                log.error("Date parse failed: {}", dateString);
                return LocalDate.of(2000, 1, 1);
            //}
        }
    }
}

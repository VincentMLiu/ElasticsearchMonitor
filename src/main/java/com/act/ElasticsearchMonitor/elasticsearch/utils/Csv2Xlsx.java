package com.act.ElasticsearchMonitor.elasticsearch.utils;

import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 文件转换工具类
 * 
 * @author wwh
 *
 */
public class Csv2Xlsx {

    public static final String DEFAULT_CHARSET = "UTF-8";

    /**
     * @param inputFile  输入的CSV文件
     * @param localXLSXFilePrefix 输出文件xlsx前缀
     * @param charset    原文件编码格式
     * @throws Exception
     */
    public static boolean convert(String inputFile, String localXLSXFilePrefix, String charset) throws Exception {
        if (StringUtils.isBlank(inputFile)) {
            System.err.println("原文件不能为空！");
            return false;
        }

        int lastIndexOf = inputFile.lastIndexOf(".");
        if (lastIndexOf == -1) {
            System.err.println("输入文件必须是.CSV格式的文件！");
            return false;
        }
        String suffix = inputFile.substring(lastIndexOf);
        if ("CSV".equalsIgnoreCase(suffix)) {
            System.err.println("输入文件必须是.CSV格式的文件！");
            return false;
        }

        // 判断文件是否存在
        File input = new File(inputFile);
        if (!input.exists()) {
            System.err.println("原文件【" + inputFile + "】不存在！");
            return false;
        }
        // 输出文件为空时默认在在当前目录生成同名的.xlsx文件
        if (StringUtils.isBlank(localXLSXFilePrefix)) {
            localXLSXFilePrefix = inputFile.subSequence(0, lastIndexOf) + "";
        }

        if (StringUtils.isBlank(charset)) {
            charset = DEFAULT_CHARSET;
        }

        return ConvertCsvToXlsx(inputFile, localXLSXFilePrefix, charset);
    }

    public static void convert(File inputFile) throws Exception {
        String sourceFile = inputFile.getAbsolutePath();
        String outFile = sourceFile.subSequence(0, sourceFile.lastIndexOf(".")) + ".xlsx";
        ConvertCsvToXlsx(sourceFile, outFile, DEFAULT_CHARSET);
    }

    public static boolean ConvertCsvToXlsx(String file, String outputXLSXFilePrefix, String charset) throws Exception {
        if (!Charset.isSupported(charset)) {
            System.out.println("不支持的字符集：" + charset);
            return false;
        }

        int dealingCount = 0;
        int outputfileIndex = 1;

        String destFile = outputXLSXFilePrefix + "-" + outputfileIndex + ".xlsx";
        SXSSFWorkbook wb = null;
        CSVReader reader = null;
        try {
            wb = new SXSSFWorkbook(1000);
            Sheet sh = wb.createSheet();
            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
//            reader = new CSVReader(new FileReader(file));
            reader = new CSVReader(in);

            int rowNum = 0;
            String[] line;
            while ((line = reader.readNext()) != null) {

                Row row = sh.createRow(rowNum++);
                for (int cellnum = 0; cellnum < line.length; cellnum++) {
                    Cell cell = row.createCell(cellnum);
                    cell.setCellValue(line[cellnum].trim());
                }
                if (rowNum % 1000 == 0) {
                    ((SXSSFSheet) sh).flushRows(1000);
                    System.out.println(rowNum);
                }

                dealingCount++;
                // 如果到了单个文件的上限 滚动新文件
                if (rowNum > 1000000 ) {
                    // 处理当前文件******************
                    if (destFile == null) {
                        destFile = file.substring(0, file.indexOf(".csv")) + "-" + outputfileIndex + ".xlsx";
                    }
                    System.out.println("Flush to file [ " + destFile + " ]");
                    FileOutputStream fileOut = new FileOutputStream(destFile);
                    wb.write(fileOut);
                    fileOut.close();
                    wb.dispose();
                    outputfileIndex++;

                    // 声明新一个工作薄
                    wb = new SXSSFWorkbook(1000);
                    // 生成一个表格
                    sh = wb.createSheet();
                    // 文件数归零
                    rowNum = 0;
                    System.out.println("NOW DEALING NUMBER : " + dealingCount);

                }

            }

            if (destFile == null) {
                destFile = file.substring(0, file.indexOf(".csv")) + "-" + outputfileIndex + ".xlsx";
            }

            ((SXSSFSheet) sh).flushRows();
            System.out.println(rowNum);
            FileOutputStream fileOut = new FileOutputStream(destFile);
            wb.write(fileOut);
            fileOut.close();
            wb.dispose();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (wb != null) {
                wb.dispose();
            }
        }
        return true;
    }

    public static List<File> combineCSV2XlSX(File[] fileList, String outputXLSXFilePrefix, String charset)  throws Exception{
        if (!Charset.isSupported(charset)) {
            System.out.println("不支持的字符集：" + charset);
            return Collections.emptyList();
        }
        if (fileList==null) {
            System.out.println("csv文件集为空");
            return Collections.emptyList();
        }

        List<File> resultXLSXFile = new ArrayList<>();

        int dealingCount = 0;
        int outputfileIndex = 1;

        String destFile = outputXLSXFilePrefix + "-" + outputfileIndex + ".xlsx";
        SXSSFWorkbook wb = null;
        CSVReader reader = null;

        wb = new SXSSFWorkbook(1000);
        Sheet sh = wb.createSheet();
        int rowNum = 0;
        try {
            for(File csvFile : fileList){
                BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), charset));
    //            reader = new CSVReader(new FileReader(file));
                reader = new CSVReader(in);

                String[] line;
                while ((line = reader.readNext()) != null) {

                    Row row = sh.createRow(rowNum++);
                    for (int cellnum = 0; cellnum < line.length; cellnum++) {
                        Cell cell = row.createCell(cellnum);
                        cell.setCellValue(line[cellnum].trim());
                    }
                    if (rowNum % 1000 == 0) {
                        ((SXSSFSheet) sh).flushRows(1000);
                        System.out.println(rowNum);
                    }

                    dealingCount++;
                    // 如果到了单个文件的上限 滚动新文件
                    if (rowNum >= 1000000 ) {
                        // 处理当前文件******************
                        System.out.println("Flush to file [ " + destFile + " ]");
                        BufferedOutputStream fileOut = new BufferedOutputStream(new FileOutputStream(destFile));

                        wb.write(fileOut);
                        fileOut.flush();
                        fileOut.close();
                        wb.dispose();

                        resultXLSXFile.add(new File(destFile));
                        //新的输出文件名
                        outputfileIndex++;
                        destFile = outputXLSXFilePrefix + "-" + outputfileIndex + ".xlsx";
                        // 声明新一个工作薄
                        wb = new SXSSFWorkbook(1000);
                        // 生成一个表格
                        sh = wb.createSheet();
                        // 文件数归零
                        rowNum = 0;
                        System.out.println("NOW DEALING NUMBER : " + dealingCount);
                    }
                }
            }

            System.out.println(dealingCount);
            System.out.println("Flush to file [ " + destFile + " ]");
            BufferedOutputStream fileOut = new BufferedOutputStream(new FileOutputStream(destFile));
            wb.write(fileOut);
            fileOut.flush();
            fileOut.close();
            wb.dispose();


        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (wb != null) {
                wb.dispose();
            }
        }

        resultXLSXFile.add(new File(destFile));

        return resultXLSXFile;

    }

}
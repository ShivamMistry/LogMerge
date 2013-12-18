package com.speed.merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

public class LogMerge {
    private static final String[] MONTHS = new String[]{"Jan", "Feb", "Mar",
            "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    private final File logFolderParent;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
            "y M d H:m:s");
    private long bytes;
    private static final String OUTPUT_DIR = "merged-logs";

    public LogMerge(final File logFolderParent) {
        this.logFolderParent = logFolderParent;
        bytes = 0;
    }

    private class MessageLine implements Comparable<MessageLine> {
        private Date date;
        private final String orig;

        private MessageLine(final String line, final int year) {
            final String[] parts = line.split(" ", 4);
            final String month = parts[0];
            int mon = 0;
            for (int i = 0; i < MONTHS.length; i++) {
                if (month.equals(MONTHS[i])) {
                    mon = i + 1;
                    break;
                }
            }
            final String day = parts[1];
            final String date = Integer.toString(year) + ' ' + mon + ' ' + day + ' ' + parts[2];
            try {
                this.date = DATE_FORMAT.parse(date);
            } catch (ParseException e) {
                System.out.println(line);
                e.printStackTrace();
            }
            orig = line;
        }

        public int compareTo(final MessageLine m) {
            return date.compareTo(m.date);
        }
    }

    public void startMerge() {
        final Set<String> names = new TreeSet<String>();
        final File[] dirs = logFolderParent.listFiles(new FileFilter() {

            public boolean accept(final File f) {
                return f.isDirectory() && !f.getName().equals(OUTPUT_DIR);
            }
        });
        for (final File dir : dirs) {
            final File[] logFiles = dir.listFiles(new FilenameFilter() {

                public boolean accept(final File dir, final String name) {
                    return name.endsWith(".log");
                }
            });
            for (final File f : logFiles) {
                names.add(f.getName());
            }
        }
        for (final String name : names) {
            System.out.println("Merging " + name);
            List<File> mergeList = new ArrayList<File>();
            for (final File dir : dirs) {
                for (final File f : dir.listFiles()) {
                    if (f.getName().equalsIgnoreCase(name)) {
                        mergeList.add(f);
                    }
                }
            }
            merge(mergeList);
        }
    }

    private void merge(final List<File> mergeList) {
        if (mergeList.size() == 0) {
            return;
        }
        if (mergeList.size() == 1) {
            final File dir = new File(logFolderParent, OUTPUT_DIR);
            if (!dir.exists()) {
                dir.mkdir();
            }
            final File toCopy = mergeList.get(0);
            final File newFile = new File(dir, toCopy.getName());
            bytes += toCopy.length();
            try {
                Files.copy(toCopy.toPath(), newFile.toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            final Queue<MessageLine> queue = new PriorityQueue<MessageLine>();
            for (final File f : mergeList) {
                bytes += f.length();
                BufferedReader read = null;
                try {
                    read = new BufferedReader(new FileReader(f));
                    String line;
                    int currYear = 2011;
                    while ((line = read.readLine()) != null) {
                        if (line.trim().isEmpty())
                            continue;
                        line = line.trim();
                        if (line.startsWith("**** BEGIN LOGGING")) {
                            String[] parts = line.split(" ");
                            String year = parts[parts.length - 1];
                            currYear = Integer.parseInt(year);
                        } else if (line.matches("\\w{3} \\d{1,2} .+")) {
                            MessageLine msg = new MessageLine(line, currYear);
                            queue.offer(msg);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (read != null) {
                            read.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            final File dir = new File(logFolderParent, "merged-logs");
            if (!dir.exists()) {
                dir.mkdir();
            }
            final File out = new File(dir, mergeList.get(0).getName());
            BufferedWriter write = null;
            try {
                write = new BufferedWriter(new FileWriter(out));
                while (!queue.isEmpty()) {
                    MessageLine s = queue.poll();
                    write.write(s.orig);
                    write.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (write != null) {
                        write.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        final long start = System.currentTimeMillis();
        final LogMerge merge = new LogMerge(new File(args[0]));
        merge.startMerge();
        final long timeToMerge = (System.currentTimeMillis() - start);
        System.out.println((merge.bytes / (1024 * 1024))
                + " megabytes of logs merged in " + timeToMerge + "ms.");
    }
}
